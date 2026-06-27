#!/usr/bin/env python3
"""
Validate that the Parquet output of convert.py faithfully represents the source SQLite.

Checks performed for each of the 4 tables:

  1. ROW COUNT MATCH
     Sum of rows across all year-partitioned Parquet files MUST equal the
     count(*) of the corresponding source SQLite table -- no rows lost,
     no rows duplicated.

  2. JSON ROUND-TRIP (only for *_details tables)
     For 100 random rows, every flattened VARCHAR column MUST equal
     json_extract_string(raw_json, '$.<key>') on the SAME row read back
     from the Parquet file. This catches conversion bugs where the wrong
     json path is used or the JSON is mangled on round-trip.
     For NUMERIC columns, the value MUST be a valid try_cast of the
     original string (or both NULL when the original is empty/unparseable).

  3. PRIMARY KEY UNIQUENESS
     internal_id MUST be unique within each Parquet table -- no row
     duplication across year partitions (sanity check that year_filter
     doesn't accidentally overlap).

  4. STATE COLUMN POPULATION
     Reports what fraction of rows have a non-NULL state. This is a
     diagnostic, not a pass/fail -- central-government tenders have no
     state axis and NULL is the honest answer.

Output: writes data_quality.json in the repo root with a per-table breakdown
and an overall `passed` boolean. Exit code: 0 if all checks pass, 1 otherwise.

Usage:
    python convert/validate.py                # validate all 4 tables
    python convert/validate.py --tables aoc_tenders  # just one
    python convert/validate.py --sample 500   # override 100-row default
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

# Reuse configuration from convert.py (single source of truth for TABLES, paths, keys).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from convert import (  # noqa: E402
    TABLES,
    ROOT,
    PARQUET_OUT,
    DATA_QUALITY_JSON,
    AOC_DETAILS_KEYS,
    VPS_DETAILS_KEYS,
    SOURCE_URL,
)

DEFAULT_SAMPLE = 100


def _open_con() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with both source SQLite DBs attached read-only."""
    con = duckdb.connect(":memory:")
    seen: set[str] = set()
    for t in TABLES:
        if t["db_path"] in seen:
            continue
        seen.add(t["db_path"])
        if not Path(t["db_path"]).exists():
            raise FileNotFoundError(f"Source SQLite missing: {t['db_path']}")
        con.execute(
            f"ATTACH '{t['db_path']}' AS {t['schema']} (TYPE SQLITE, READ_ONLY)"
        )
    return con


def _row_count_sqlite(con: duckdb.DuckDBPyConnection, table_cfg: dict) -> int:
    """Source row count, excluding the known test row in tender_details.

    Must match convert.py's filter so the parquet-vs-sqlite equality check
    in validate_table holds.
    """
    return con.execute(
        f"SELECT count(*) FROM {table_cfg['schema']}.{table_cfg['source_table']} "
        f"WHERE internal_id <> 'test_id_1'"
    ).fetchone()[0]


def _parquet_glob(table_cfg: dict) -> list[Path]:
    """All year-partitioned Parquet files for this table."""
    root = PARQUET_OUT / table_cfg["name"]
    if not root.exists():
        return []
    return sorted(root.glob("year=*/*.parquet"))


def _row_count_parquet(con: duckdb.DuckDBPyConnection, files: list[Path]) -> int:
    """Sum of rows across all parquet files via a glob read."""
    if not files:
        return 0
    glob = (PARQUET_OUT / files[0].relative_to(PARQUET_OUT).parents[0]).as_posix() + "/year=*/data.parquet"
    # Use the per-table directory for the glob
    table_name = files[0].relative_to(PARQUET_OUT).parts[0]
    glob = f"{PARQUET_OUT.as_posix()}/{table_name}/year=*/data.parquet"
    return con.execute(
        f"SELECT count(*) FROM read_parquet('{glob}', union_by_name=true)"
    ).fetchone()[0]


def _primary_key_unique(con: duckdb.DuckDBPyConnection, table_cfg: dict) -> tuple[int, int]:
    """Returns (distinct_pk_count, total_count). Should be equal."""
    pk = table_cfg["primary_key"]
    table_name = table_cfg["name"]
    glob = f"{PARQUET_OUT.as_posix()}/{table_name}/year=*/data.parquet"
    total, distinct = con.execute(
        f"SELECT count(*), count(DISTINCT {pk}) "
        f"FROM read_parquet('{glob}', union_by_name=true)"
    ).fetchone()
    return distinct, total


def _state_population(con: duckdb.DuckDBPyConnection, table_cfg: dict) -> tuple[int, int]:
    """Returns (rows_with_state, total_rows)."""
    table_name = table_cfg["name"]
    glob = f"{PARQUET_OUT.as_posix()}/{table_name}/year=*/data.parquet"
    non_null, total = con.execute(
        f"SELECT count(state), count(*) "
        f"FROM read_parquet('{glob}', union_by_name=true)"
    ).fetchone()
    return non_null, total


def _json_round_trip(
    con: duckdb.DuckDBPyConnection,
    table_cfg: dict,
    json_keys: list[tuple[str, str, str | None]],
    sample_size: int,
) -> dict:
    """
    For `sample_size` random rows from the Parquet file, verify that:
      - VARCHAR flattened columns equal json_extract_string(raw_json, '$."<key>"')
      - Numeric flattened columns equal try_cast(json_extract_string(...) AS <type>)
        when the original is parseable, and both are NULL otherwise.

    Returns a dict with the sample size, number of mismatches, and details.
    """
    table_name = table_cfg["name"]
    glob = f"{PARQUET_OUT.as_posix()}/{table_name}/year=*/data.parquet"

    # Build the comparison SQL. We compute one mismatch flag per JSON key in
    # an inner CTE, then sum the flags in the outer query. We tried inlining
    # the sum in the same SELECT (`CASE ... END AS _m_x + CASE ... END AS _m_y
    # + ... AS _mismatch_count`) but DuckDB v1.5 rejects the long `+` chain
    # of aliased CASE expressions -- the parser interprets `+` as a
    # between-SELECT-item operator and chokes. Splitting the work across two
    # CTEs avoids the ambiguity entirely.

    flag_selects: list[str] = []
    flag_aliases: list[str] = []
    detail_cols: list[str] = []
    for i, (jkey, out_col, typ) in enumerate(json_keys):
        json_path = f'$."{jkey}"'
        raw = f"json_extract_string(raw_json, '{json_path}')"
        flat = out_col
        if typ:
            # Numeric round-trip must mirror the conversion's clean_amount /
            # clean_int regex strip -- otherwise we'd falsely flag every row
            # where the raw JSON had a currency prefix (e.g. "₹ 20441") that
            # was stripped before casting. We replicate the same regex here.
            if typ == "DOUBLE":
                cleaned_raw = f"regexp_replace({raw}, '[^0-9.-]', '', 'g')"
            elif typ == "INTEGER":
                cleaned_raw = f"regexp_replace({raw}, '[^0-9-]', '', 'g')"
            else:
                cleaned_raw = raw
            cast_expr = f"try_cast(NULLIF({cleaned_raw}, '') AS {typ})"
            check_expr = (
                f"(({flat} IS NULL AND ({cast_expr} IS NULL)) "
                f"OR ({flat} IS NOT NULL AND {cast_expr} IS NOT NULL "
                f"AND {cast_expr} = {flat}))"
            )
        else:
            check_expr = f"(({flat} IS NULL AND {raw} IS NULL) OR ({flat} = {raw}))"
        flag_alias = f"_m_{i}"
        flag_aliases.append(flag_alias)
        flag_selects.append(f"(CASE WHEN NOT ({check_expr}) THEN 1 ELSE 0 END) AS {flag_alias}")
        detail_cols.append(flag_alias)

    sum_expr = " + ".join(f"COALESCE({a}, 0)" for a in flag_aliases) or "0"
    full_sql = f"""
        WITH sampled AS (
          SELECT *
          FROM read_parquet('{glob}', union_by_name=true)
          USING SAMPLE {sample_size}
        ),
        flags AS (
          SELECT
            internal_id,
            raw_json,
            {', '.join(flag_selects)}
          FROM sampled
        ),
        with_checks AS (
          SELECT
            internal_id,
            raw_json,
            ({sum_expr}) AS _mismatch_count,
            {', '.join(detail_cols)}
          FROM flags
        )
        SELECT
          count(*) AS sample_size,
          count(*) FILTER (WHERE _mismatch_count = 0) AS clean_rows,
          sum(_mismatch_count) AS total_mismatches,
          (SELECT list(struct_pack(internal_id, raw_json, mismatches := list_filter(
              [{', '.join(f"struct_pack(col := '{c}', mismatch := {c})" for c in detail_cols)}],
              x -> x.mismatch = 1
           ))) FROM with_checks WHERE _mismatch_count > 0 LIMIT 5) AS sample_mismatches
        FROM with_checks
    """
    row = con.execute(full_sql).fetchone()
    sample_size_actual, clean_rows, total_mismatches, sample_mismatches = row
    return {
        "sample_size": int(sample_size_actual or 0),
        "clean_rows": int(clean_rows or 0),
        "total_mismatches": int(total_mismatches or 0),
        "sample_mismatches": sample_mismatches,
    }


def validate_table(
    con: duckdb.DuckDBPyConnection,
    table_cfg: dict,
    sample_size: int,
) -> dict:
    """Run all checks for one table. Returns a result dict for data_quality.json."""
    name = table_cfg["name"]
    print(f"\n=== {name} ===", flush=True)

    parquet_files = _parquet_glob(table_cfg)
    if not parquet_files:
        msg = f"No parquet files found under {PARQUET_OUT / name}/year=*/*.parquet"
        print(f"  FAIL: {msg}", file=sys.stderr)
        return {
            "table": name,
            "passed": False,
            "error": msg,
            "checks": {},
        }

    src_count = _row_count_sqlite(con, table_cfg)
    parq_count = _row_count_parquet(con, parquet_files)
    pk_distinct, pk_total = _primary_key_unique(con, table_cfg)
    state_present, state_total = _state_population(con, table_cfg)

    row_count_match = src_count == parq_count
    pk_unique = pk_distinct == pk_total

    print(f"  source rows: {src_count:,}", flush=True)
    print(f"  parquet rows: {parq_count:,}", flush=True)
    print(f"  row_count_match: {row_count_match}", flush=True)
    print(f"  pk unique: {pk_distinct:,} distinct / {pk_total:,} total", flush=True)
    print(f"  state populated: {state_present:,} / {state_total:,} "
          f"({(100.0 * state_present / state_total):.1f}%)", flush=True)

    checks: dict = {
        "row_count": {
            "source_rows": src_count,
            "parquet_rows": parq_count,
            "passed": row_count_match,
        },
        "primary_key_unique": {
            "distinct": pk_distinct,
            "total": pk_total,
            "passed": pk_unique,
        },
        "state_population": {
            "non_null": state_present,
            "total": state_total,
            "fraction": round(state_present / state_total, 4) if state_total else 0.0,
        },
    }

    if table_cfg["json_keys"]:
        json_keys = table_cfg["json_keys"]
        rt = _json_round_trip(con, table_cfg, json_keys, sample_size)
        checks["json_round_trip"] = {
            "sample_size": rt["sample_size"],
            "clean_rows": rt["clean_rows"],
            "total_mismatches": rt["total_mismatches"],
            "passed": rt["total_mismatches"] == 0,
        }
        print(f"  json_round_trip: {rt['clean_rows']}/{rt['sample_size']} clean "
              f"({rt['total_mismatches']} mismatches)", flush=True)
        if rt["sample_mismatches"]:
            print(f"  first mismatches:", flush=True)
            for s in rt["sample_mismatches"][:5]:
                print(f"    internal_id={s['internal_id']} mismatched={s['mismatches']}", flush=True)

    table_passed = all(c.get("passed", True) for c in checks.values())
    return {
        "table": name,
        "passed": table_passed,
        "checks": checks,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--tables", nargs="*", default=None,
                        help="Subset of tables to validate (default: all 4)")
    parser.add_argument("--sample", type=int, default=DEFAULT_SAMPLE,
                        help=f"JSON round-trip sample size (default {DEFAULT_SAMPLE})")
    parser.add_argument("--out", type=str, default=str(DATA_QUALITY_JSON),
                        help="Path for data_quality.json output")
    args = parser.parse_args()

    print(f"Validating Parquet output under {PARQUET_OUT.relative_to(ROOT)}/", flush=True)
    print(f"Sample size: {args.sample}", flush=True)

    # Sanity: parquet directory must exist
    if not PARQUET_OUT.exists():
        print(f"ERROR: {PARQUET_OUT} does not exist. Run convert.py first.", file=sys.stderr)
        return 1

    con = _open_con()

    tables = TABLES
    if args.tables:
        wanted = set(args.tables)
        tables = [t for t in TABLES if t["name"] in wanted]
        if not tables:
            print(f"ERROR: no tables matched {args.tables!r}", file=sys.stderr)
            return 1

    results: list[dict] = []
    for t in tables:
        results.append(validate_table(con, t, args.sample))

    con.close()

    overall_pass = all(r["passed"] for r in results)

    report = {
        "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source_url": SOURCE_URL,
        "parquet_root": PARQUET_OUT.relative_to(ROOT).as_posix(),
        "sample_size": args.sample,
        "overall_passed": overall_pass,
        "tables": results,
    }

    out_path = Path(args.out)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"\nWrote {out_path.relative_to(ROOT) if out_path.is_absolute() and out_path.is_relative_to(ROOT) else out_path}",
          flush=True)

    # Final summary
    print("\nValidation summary:")
    for r in results:
        mark = "PASS" if r["passed"] else "FAIL"
        print(f"  [{mark}] {r['table']}")
    print(f"\nOverall: {'PASS' if overall_pass else 'FAIL'}")

    return 0 if overall_pass else 1


if __name__ == "__main__":
    sys.exit(main())
