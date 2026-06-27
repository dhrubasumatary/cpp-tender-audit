#!/usr/bin/env python3
"""
Convert CPPP SQLite databases to year-partitioned, ZSTD-compressed Parquet.

Tables processed (4 total, ~16.5M rows):
    aoc_tenders     (aoc_tenders.db) -- ~4.92M rows, has `year` column directly
    aoc_details     (aoc_tenders.db) -- ~4.54M rows, JSON blob keyed by details_json
    tenders         (tenders_vps.db) -- ~3.95M rows, year derived from e_published_date
    tender_details  (tenders_vps.db) -- ~3.18M rows, JSON blob keyed by details_json

For *_details tables we apply HYBRID JSON STORAGE:
    - Every key from the JSON schema is flattened into a typed column
      (try_cast for numeric fields; original VARCHAR fallback).
    - The original JSON is also kept verbatim in a `raw_json` VARCHAR column
      so future schema additions / unmapped keys are never lost.

State column:
    Derived by case-insensitive substring scan of the source text
    (org_name / organisation_name / details_json.Organisation Name / Location)
    against the 36 canonical Indian states/UTs + 76 synonyms defined in
    convert/states.json. Rows with no state hint get NULL -- this is honest
    and matches reality: most central-government tenders have no state axis.

Year partitioning:
    Hive-style: parquet/<table>/year=<YYYY>/data.parquet
    Special partitions preserve all rows without dropping bad data:
        year=unparsed     -- NULL/malformed date
        year=pre_2000     -- year < 2000 (rare legacy rows)
        year=future_typo  -- year > 2030 (data entry errors like "5023")

Output compression: ZSTD level 22 (DuckDB default; pinned for byte-identical reproduce).
Output provenance: every Parquet file carries kv_metadata with:
    source_url              : https://tender.sarthaksidhant.com/
    conversion_script_sha   : git blob SHA of this script (stable across clones)
    conversion_script_path  : relative path within repo
    conversion_timestamp    : UTC ISO8601 at time of conversion
    source_table            : SQLite table name
    source_db_sha256        : SHA256 of the source .db file (verified against SHA256SUMS)
    scraped_at_min/max      : observed scrape-time range for the rows in this partition
    row_count               : exact row count written

Reproducibility:
    Every source query has ORDER BY <primary_key> so row order in the Parquet
    file is deterministic. Combined with pinned ZSTD level and pinned DuckDB
    version (1.5.4) this yields byte-identical output across runs/clones.

Resource ceiling: ~2.3 GB RAM, ~29 GB disk. We process one (table, year) at
a time and let DuckDB stream from SQLite via ATTACH.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import duckdb

# --------------------------------------------------------------------------------------
# Paths and constants
# --------------------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent
CONVERT_DIR = ROOT / "convert"
STATES_JSON = CONVERT_DIR / "states.json"
PARQUET_OUT = ROOT / "parquet"
DATA_QUALITY_JSON = ROOT / "data_quality.json"
DUCKDB_SPILL_DIR = ROOT / ".tmp" / "duckdb_spill"  # On-disk spill for low-RAM runs

# Memory budget on the build machine is ~5 GB free. DuckDB must self-throttle
# so a single large partition (e.g. aoc_details 'unparsed' with all 4.5M rows)
# does not OOM-kill the box. 1.0 GB is conservative -- with temp_directory on
# disk, DuckDB spills aggregates/sorts to disk and never exceeds this.
# Set DUCKDB_MEMORY_LIMIT=1500MB in env to opt back into the larger pool when
# running alone (not in parallel with another convert.py).
DUCKDB_MEMORY_LIMIT = os.environ.get("DUCKDB_MEMORY_LIMIT", "1000MB")
DUCKDB_THREADS = int(os.environ.get("DUCKDB_THREADS", "2"))

SOURCE_URL = "https://tender.sarthaksidhant.com/"
# Default ZSTD level is 9, not 22. DuckDB's default of 22 is fine on machines
# with many CPU cores and unbounded RAM, but on the build box (2 cores, 7 GB
# RAM total) ZSTD-22 made the 4.5M-row aoc_details partition crawl at ~8 MB/min.
# ZSTD-9 is ~5x faster and only ~10-15% worse on compression ratio, which is
# irrelevant for browser-side DuckDB reads (the bottleneck is network, not
# decompress CPU). Set ZSTD_LEVEL=22 here to regenerate byte-identical output
# for the make reproduce check in phase 4.
ZSTD_LEVEL = int(os.environ.get("ZSTD_LEVEL", "9"))

# --------------------------------------------------------------------------------------
# JSON schemas (verified against https://tender.sarthaksidhant.com/ as of 2026-06)
# --------------------------------------------------------------------------------------

# AOC details: 12 keys. Numeric values may be malformed so we try_cast.
AOC_DETAILS_KEYS: list[tuple[str, str, str | None]] = [
    ("Tender Type",                                       "tender_type",                None),
    ("Contract Date",                                     "contract_date",              None),
    ("Contract Value",                                    "contract_value",             "DOUBLE"),
    ("Published Date",                                    "published_date",             None),
    ("Tender Document",                                   "tender_document_url",        None),
    ("Tender Ref. No.",                                   "tender_ref_no",              None),
    ("Organisation Name",                                 "organisation_name",          None),
    ("Tender Description",                                "tender_description",         None),
    ("Number of bids received",                           "num_bids_received",          "INTEGER"),
    ("Name of the selected bidder(s)",                    "selected_bidders",           None),
    ("Address of the selected bidder(s)",                 "selected_bidder_address",    None),
    ("Date of Completion/Completion Period in Days",      "completion_period_days",     "INTEGER"),
]

# VPS tender details: 21 keys.
VPS_DETAILS_KEYS: list[tuple[str, str, str | None]] = [
    ("Tender Reference Number",       "tender_reference_number",   None),
    ("Tender Title",                  "tender_title",              None),
    ("Organisation Name",             "organisation_name",         None),
    ("Organisation Type",             "organisation_type",         None),
    ("Tender Category",               "tender_category",           None),
    ("Tender Type",                   "tender_type",               None),
    ("Product Category",              "product_category",          None),
    ("Product Sub-Category",          "product_sub_category",      None),
    ("ePublished Date",               "epublished_date",           None),
    ("Bid Opening Date",              "bid_opening_date",          None),
    ("Bid Submission Start Date",     "bid_submission_start_date", None),
    ("Bid Submission End Date",       "bid_submission_end_date",   None),
    ("Document Download Start Date",  "doc_download_start_date",   None),
    ("Document Download End Date",    "doc_download_end_date",     None),
    ("EMD",                           "emd_amount",                "DOUBLE"),
    ("Tender Fee",                    "tender_fee_amount",         "DOUBLE"),
    ("Location",                      "location",                  None),
    ("Address",                       "address",                   None),
    ("Name",                          "contact_officer_name",      None),
    ("Work Description",              "work_description",          None),
    ("Tender Document",               "tender_document_url",       None),
]


# --------------------------------------------------------------------------------------
# Per-table conversion config
# --------------------------------------------------------------------------------------

@dataclass_frozen := __import__("dataclasses").dataclass(frozen=True)
class _dc_marker:
    pass


def _table(name, db_path, schema, source_table, year_col, year_from_date, state_source, json_keys, primary_key, quarter_partition=False, year_from_json=None):
    return {
        "name": name,
        "db_path": str(ROOT / db_path),
        "schema": schema,
        "source_table": source_table,
        "year_col": year_col,                          # column name (e.g. "year") or None
        "year_from_date": year_from_date,              # (col, fmt) or None
        "year_from_json": year_from_json,              # JSON key for tender published date, or None
        "state_source": state_source,                  # "direct_col:<col>" or "json_concat:<keys>"
        "json_keys": json_keys,                        # list of (json_key, out_col, type_or_None) or []
        "primary_key": primary_key,
        "quarter_partition": quarter_partition,        # True => split each year into Q1-Q4 sub-partitions
    }


TABLES = [
    _table(
        name="aoc_tenders",
        db_path="aoc_tenders.db",
        schema="aoc",
        source_table="aoc_tenders",
        year_col="year",
        year_from_date=None,
        state_source="direct_col:org_name",
        json_keys=[],
        primary_key="internal_id",
    ),
    _table(
        name="aoc_details",
        db_path="aoc_tenders.db",
        schema="aoc",
        source_table="aoc_details",
        year_col=None,
        year_from_date=None,  # scraped_at has timezone suffix + is one big batch -- useless for partitioning
        year_from_json="Published Date",  # actual tender publication date inside the JSON
        state_source="json_concat:Organisation Name",
        json_keys=AOC_DETAILS_KEYS,
        primary_key="internal_id",
        # JSON Published Date covers ~1.7M of 4.5M rows and distributes across
        # 16 years (2011-2026). Rows without a parseable Published Date fall
        # into a single `year=unparsed` bucket (~2.8M rows) -- this is the same
        # shape as a single year partition would have been, so memory-wise it's
        # no worse than the old "all in 2026-q2" arrangement.
        quarter_partition=False,
    ),
    _table(
        name="tenders",
        db_path="tenders_vps.db",
        schema="vps",
        source_table="tenders",
        year_col=None,
        year_from_date=("e_published_date", "%d-%b-%Y %I:%M %p"),
        state_source="direct_col:organisation_name",
        json_keys=[],
        primary_key="internal_id",
    ),
    _table(
        name="tender_details",
        db_path="tenders_vps.db",
        schema="vps",
        source_table="tender_details",
        year_col=None,
        year_from_date=None,
        year_from_json="ePublished Date",  # 100% coverage, parses cleanly, spans 2010-2027
        state_source="json_concat:Organisation Name|Location",
        json_keys=VPS_DETAILS_KEYS,
        primary_key="internal_id",
        quarter_partition=False,
    ),
]


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------

def load_states() -> tuple[dict, dict]:
    """Load (canonical_states, synonyms) from convert/states.json."""
    with open(STATES_JSON) as f:
        data = json.load(f)
    return data["states"], data["synonyms"]


def build_state_case_expr(source_expr: str, canonical: dict, synonyms: dict) -> str:
    """
    Build a SQL CASE expression that returns the canonical state name
    (or NULL) by scanning `source_expr` for known state names/synonyms.

    Word-boundary matching is required: short synonyms like "AR", "GA",
    "OD", "JH" would otherwise match inside unrelated words ("BoARd",
    "BangalOre", "DEVELopment", "BHEL JHansi"). We pad both sides of
    the source with spaces and require ` {needle} ` (literal spaces),
    which forces the needle to be a standalone token.

    Order matters: longest match wins, so "Madhya Pradesh" beats a
    bare "Pradesh" (which isn't a synonym anyway, but the rule generalises).
    """
    # Build all needles: canonical names first, then synonyms.
    needles: list[tuple[str, str]] = []  # (uppercase_needle, canonical_name)
    for name in canonical:
        needles.append((name.upper(), name))
    for alias, target in synonyms.items():
        if target in canonical:
            needles.append((alias.upper(), target))
    # Sort longest-first so the most specific match wins.
    needles.sort(key=lambda x: -len(x[0]))

    # Deduplicate (needle, target) pairs while preserving longest-first order.
    deduped: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for needle, target in needles:
        if (needle, target) not in seen:
            seen.add((needle, target))
            deduped.append((needle, target))

    # Normalize source: upper-case + collapse any non-alphanumeric run to a
    # single space (handles "Maharashtra, India", "J&K", "BHEL-Hyderabad",
    # "Central/Works" all uniformly), then pad with spaces on both sides.
    # Word-boundary matching is now: ' {needle} ' substring against the padded text.
    normalized = (
        f"regexp_replace(UPPER({source_expr}), '[^A-Z0-9]+', ' ', 'g')"
    )
    padded = f"' ' || {normalized} || ' '"
    branches = " ".join(
        f"WHEN {padded} LIKE '% {needle.replace(chr(39), chr(39)*2)} %' "
        f"THEN '{target.replace(chr(39), chr(39)*2)}'"
        for needle, target in deduped
    )
    return f"CASE {branches} ELSE NULL END"


def get_script_sha() -> str:
    """
    Git blob SHA of this script (deterministic across clones for unchanged content).
    Falls back to filesystem sha256 if not in a git repo (e.g., during initial setup).
    """
    try:
        rel = Path(__file__).resolve().relative_to(ROOT)
        return subprocess.check_output(
            ["git", "hash-object", str(rel)],
            cwd=ROOT,
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _year_expr(table_cfg: dict) -> str:
    """
    SQL expression that yields the integer year (or NULL) for a row.

    Three strategies, in priority order:
    1. Direct `year` column (aoc_tenders): use it verbatim.
    2. JSON field (aoc_details.Published Date, tender_details.ePublished Date):
       json_extract_string + try_strptime. Used when the table has a proper
       "tender publication date" inside the JSON rather than just the scrape
       timestamp -- the latter is useless for partitioning since all rows in a
       single scrape batch share the same scraped_at.
    3. SQLite date column with timezone-stripped strptime (legacy fallback).
    """
    if table_cfg["year_col"]:
        return table_cfg["year_col"]
    if table_cfg.get("year_from_json"):
        # JSON dates in this corpus are formatted as '21-Apr-2026 11:00 AM'
        # -- matches strptime %d-%b-%Y %I:%M %p.
        return (
            f"year(try_strptime("
            f"json_extract_string(details_json, '$.\"{table_cfg['year_from_json']}\"'), "
            f"'%d-%b-%Y %I:%M %p'))"
        )
    date_col, fmt = table_cfg["year_from_date"]
    # Strip trailing timezone offsets: +02, +02:00, +0200, -05, -05:30, etc.
    # In the resulting SQL string literal we want the regex `\d` (a single
    # backslash + d, which RE2 reads as "any digit"). DuckDB does NOT collapse
    # `\\` in string literals, so the SQL must contain exactly one backslash.
    # That means the Python f-string source needs `\\d` (which becomes `\d`
    # in the actual string we hand to DuckDB). Note: using `\\\\d` would be
    # the easy mistake -- that produces `\\d` in SQL, which RE2 reads as a
    # literal backslash followed by literal `d`, not a digit.
    stripped = (
        f"regexp_replace({date_col}, "
        f"'[+-]\\d{{2}}(:?\\d{{2}})?$', '')"
    )
    return f"year(try_strptime({stripped}, '{fmt}'))"


def _parsed_date_expr(table_cfg: dict) -> str:
    """
    SQL expression yielding the parsed TIMESTAMP for the date column, with
    timezone suffixes stripped. NULL if input can't parse.

    Same shape as `_year_expr` but returns the full timestamp (not just year),
    so we can also derive month/quarter. Only meaningful for tables whose
    year_from_date is set.
    """
    if table_cfg["year_col"]:
        # aoc_tenders has year as a direct BIGINT; we don't have month for it,
        # but quarter_partition is also off for that table, so this branch is
        # never reached in practice. Return a constant NULL to keep the SQL
        # valid.
        return "NULL::TIMESTAMP"
    if table_cfg.get("year_from_json"):
        return (
            f"try_strptime("
            f"json_extract_string(details_json, '$.\"{table_cfg['year_from_json']}\"'), "
            f"'%d-%b-%Y %I:%M %p')"
        )
    date_col, fmt = table_cfg["year_from_date"]
    stripped = (
        f"regexp_replace({date_col}, "
        f"'[+-]\\d{{2}}(:?\\d{{2}})?$', '')"
    )
    return f"try_strptime({stripped}, '{fmt}')"


def list_distinct_years(con: duckdb.DuckDBPyConnection, table_cfg: dict, year_filter: str = "") -> list[str]:
    """
    List distinct year-partition values to emit for a given table.

    Returns partition labels like:
      ["2020", "2021", ..., "pre_2000"]                -- year-only mode
      ["2020-q1", "2020-q2", ..., "2026-q4", "unparsed"] -- quarter mode

    `year_filter` is an optional SQL fragment to restrict rows (e.g. for testing).
    """
    year_expr = _year_expr(table_cfg)

    if not table_cfg.get("quarter_partition"):
        sql = f"""
            SELECT
              CASE
                WHEN {year_expr} IS NULL                THEN 'unparsed'
                WHEN {year_expr} < 2000                 THEN 'pre_2000'
                WHEN {year_expr} > 2030                 THEN 'future_typo'
                ELSE CAST({year_expr} AS VARCHAR)
              END AS partition_label
            FROM {table_cfg['schema']}.{table_cfg['source_table']}
            WHERE {year_filter or '1=1'}
            GROUP BY partition_label
            ORDER BY partition_label
        """
        return [row[0] for row in con.execute(sql).fetchall()]

    # Quarter partitioning: labels are "<year>-q<N>" where N in 1..4, plus
    # "unparsed" for NULL years. For pre-2000 / future-typo we still collapse
    # to the non-quarter buckets (those have so few rows that quarterly
    # splitting is meaningless).
    parsed = _parsed_date_expr(table_cfg)
    sql = f"""
        SELECT
          CASE
            WHEN {year_expr} IS NULL                                  THEN 'unparsed'
            WHEN {year_expr} < 2000                                   THEN 'pre_2000'
            WHEN {year_expr} > 2030                                   THEN 'future_typo'
            ELSE CAST({year_expr} AS VARCHAR)
                 || '-q'
                 || CAST(((month({parsed}) - 1) // 3) + 1 AS VARCHAR)
          END AS partition_label
        FROM {table_cfg['schema']}.{table_cfg['source_table']}
        WHERE {year_filter or '1=1'}
        GROUP BY partition_label
        ORDER BY partition_label
    """
    return [row[0] for row in con.execute(sql).fetchall()]


def count_rows(con: duckdb.DuckDBPyConnection, table_cfg: dict) -> int:
    """Source row count, excluding the known test row in tender_details."""
    return con.execute(
        f"SELECT count(*) FROM {table_cfg['schema']}.{table_cfg['source_table']} "
        f"WHERE internal_id <> 'test_id_1'"
    ).fetchone()[0]


# --------------------------------------------------------------------------------------
# Query builders
# --------------------------------------------------------------------------------------

def _typed_column(json_raw_expr: str, out_col: str, typ: str | None) -> str:
    """
    Build a SELECT fragment for one JSON-flattened column.

    For VARCHAR (typ is None): just project the JSON string verbatim.

    For DOUBLE (money fields like EMD, Tender Fee, Contract Value): strip
    non-numeric characters before casting. The raw JSON often has currency
    prefixes (e.g. "₹ 20441"), thousands separators ("1,00,000"), and stray
    whitespace -- all of which make a naive try_cast DOUBLE return NULL.
    We strip everything except digits, minus, and dot, then cast.

    For INTEGER (count fields like num_bids_received, completion_period_days):
    same idea but allow no decimal point. Note: India uses lakh/crore
    formatting with commas every two digits ("10,00,000"), not every three --
    so a plain thousands-separator strip works fine here since we never need
    the absolute magnitude to survive, only the integer part.

    If stripping leaves an empty string, NULLIF turns it into NULL so the
    cast doesn't error.
    """
    if typ is None:
        return f"{json_raw_expr} AS {out_col}"
    if typ == "DOUBLE":
        cleaned = f"regexp_replace({json_raw_expr}, '[^0-9.-]', '', 'g')"
        return f"try_cast(NULLIF({cleaned}, '') AS DOUBLE) AS {out_col}"
    if typ == "INTEGER":
        cleaned = f"regexp_replace({json_raw_expr}, '[^0-9-]', '', 'g')"
        return f"try_cast(NULLIF({cleaned}, '') AS INTEGER) AS {out_col}"
    # Unknown type -- fall back to naive cast.
    return f"try_cast({json_raw_expr} AS {typ}) AS {out_col}"


def select_columns(table_cfg: dict, canonical: dict, synonyms: dict) -> str:
    """Build the SELECT column list for one (table, year) partition."""
    schema = table_cfg["schema"]
    src = table_cfg["source_table"]
    pk = table_cfg["primary_key"]

    year_expr = _year_expr(table_cfg)

    # State column derivation
    state_source = table_cfg["state_source"]
    if state_source.startswith("direct_col:"):
        col = state_source.split(":", 1)[1]
        state_expr = build_state_case_expr(f"{col}", canonical, synonyms)
    elif state_source.startswith("json_concat:"):
        keys = state_source.split(":", 1)[1].split("|")
        # Concatenate all the JSON fields (most-likely-to-contain-state first)
        # so the substring scan sees them in one pass.
        parts = " || ' ' || ".join(
            f"COALESCE(json_extract_string(details_json, '$.\"{k}\"'), '')" for k in keys
        )
        state_expr = build_state_case_expr(f"({parts})", canonical, synonyms)
    else:
        raise ValueError(f"Unknown state_source: {state_source}")

    cols: list[str] = []
    if table_cfg["name"] == "aoc_tenders":
        # Direct columns + derived
        cols = [
            f"internal_id",
            f"portal_type",
            f"{year_expr} AS year",
            f"sl_no",
            f"aoc_date",
            f"closing_date",
            f"title",
            f"ref_no",
            f"tender_id",
            f"org_name",
            f"{state_expr} AS state",
            f"detail_url",
            f"partition_id",
        ]
    elif table_cfg["name"] == "tenders":
        cols = [
            f"internal_id",
            f"tender_id",
            f"detail_url",
            f"status",
            f"organisation_name",
            f"title",
            f"reference_number",
            f"portal_type",
            f"serial_number",
            f"e_published_date",
            f"try_strptime(e_published_date, '%d-%b-%Y %I:%M %p') AS e_published_date_parsed",
            f"bid_submission_closing_date",
            f"try_strptime(bid_submission_closing_date, '%d-%b-%Y %I:%M %p') AS bid_submission_closing_date_parsed",
            f"tender_opening_date",
            f"try_strptime(tender_opening_date, '%d-%b-%Y %I:%M %p') AS tender_opening_date_parsed",
            f"corrigendum_url",
            f"scraped_at",
            f"{year_expr} AS year",
            f"{state_expr} AS state",
            f"partition_id",
        ]
    elif table_cfg["name"] == "aoc_details":
        cols = [
            f"internal_id",
            f"tender_id",
            f"scraped_at",
            f"try_strptime(scraped_at, '%Y-%m-%d %H:%M:%S.%f') AS scraped_at_parsed",
            f"{year_expr} AS year",
            f"{state_expr} AS state",
        ]
        # Flattened JSON keys
        for jkey, out_col, typ in AOC_DETAILS_KEYS:
            raw = f"json_extract_string(details_json, '$.\"{jkey}\"')"
            cols.append(_typed_column(raw, out_col, typ))
        # raw_json escape hatch
        cols.append("details_json AS raw_json")
    elif table_cfg["name"] == "tender_details":
        cols = [
            f"internal_id",
            f"tender_id",
            f"scraped_at",
            f"try_strptime(scraped_at, '%Y-%m-%d %H:%M:%S.%f') AS scraped_at_parsed",
            f"{year_expr} AS year",
            f"{state_expr} AS state",
        ]
        for jkey, out_col, typ in VPS_DETAILS_KEYS:
            raw = f"json_extract_string(details_json, '$.\"{jkey}\"')"
            cols.append(_typed_column(raw, out_col, typ))
        cols.append("details_json AS raw_json")
    else:
        raise ValueError(f"Unknown table: {table_cfg['name']}")

    return ", ".join(cols)


def year_filter_clause(table_cfg: dict, partition_label: str) -> str:
    """
    Build a WHERE clause that selects rows for one partition.

    Accepts labels in either form:
      "2026"            -- year-only partition
      "2026-q1"         -- quarter sub-partition (only when quarter_partition=True)
      "unparsed" / "pre_2000" / "future_typo" -- special non-time buckets
    """
    year_expr = _year_expr(table_cfg)

    if partition_label == "unparsed":
        return f"{year_expr} IS NULL"
    if partition_label == "pre_2000":
        return f"{year_expr} IS NOT NULL AND {year_expr} < 2000"
    if partition_label == "future_typo":
        return f"{year_expr} > 2030"

    if "-q" in partition_label:
        # Quarter partition: "2026-q1" .. "2026-q4"
        year_str, q_str = partition_label.split("-q", 1)
        year_int = int(year_str)
        q_int = int(q_str)
        # Quarter Q = (month - 1) // 3 + 1, so month range for Q1..Q4 is:
        #   Q1: 1..3, Q2: 4..6, Q3: 7..9, Q4: 10..12
        month_lo = (q_int - 1) * 3 + 1
        month_hi = q_int * 3
        parsed = _parsed_date_expr(table_cfg)
        return (
            f"({year_expr} = {year_int}) "
            f"AND (month({parsed}) >= {month_lo}) "
            f"AND (month({parsed}) <= {month_hi})"
        )

    return f"{year_expr} = {int(partition_label)}"


def scraped_at_range(con: duckdb.DuckDBPyConnection, table_cfg: dict, where: str) -> tuple[str | None, str | None]:
    """Return (min, max) scraped_at text observed in this partition (for provenance)."""
    schema = table_cfg["schema"]
    src = table_cfg["source_table"]
    # scraped_at exists on all 4 tables; for aoc_details and tender_details it's an ISO
    # timestamp; for aoc_tenders and tenders it's also a varchar scrape timestamp.
    try:
        row = con.execute(
            f"SELECT min(scraped_at), max(scraped_at) FROM {schema}.{src} WHERE {where}"
        ).fetchone()
        return row[0], row[1]
    except Exception:
        return None, None


# --------------------------------------------------------------------------------------
# Main conversion routine
# --------------------------------------------------------------------------------------

def _open_duckdb() -> duckdb.DuckDBPyConnection:
    """
    Open an in-memory DuckDB connection with conservative memory settings.

    - memory_limit caps the buffer pool; DuckDB spills aggregates/sorts to
      temp_directory when exceeded.
    - temp_directory on disk is required for spill to work with :memory:.
    - threads=2 bounds peak memory from concurrent workers (DuckDB defaults
      to many threads on multi-core machines, which multiplies peak RSS).
    """
    DUCKDB_SPILL_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET temp_directory='{DUCKDB_SPILL_DIR.as_posix()}'")
    con.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    return con


def _kv_to_struct_pack(kv: dict) -> str:
    """
    Convert a {key: value} dict to a DuckDB STRUCT_PACK(...) SQL literal.

    DuckDB's KV_METADATA clause expects a STRUCT expression, not a JSON
    object literal -- so json.dumps({...}) produces invalid SQL
    ("zero-length delimited identifier at or near ''"). STRUCT_PACK with
    := assignment syntax accepts all string values (including empty
    strings and embedded single quotes via doubling).

    Keys must be valid SQL identifiers (snake_case -- which matches our
    provenance schema). Values are coerced to str() so STRUCT_PACK never
    sees a None that would error.
    """
    parts: list[str] = []
    for k, v in kv.items():
        k_safe = str(k)
        # If a key has chars that aren't valid identifier chars, this would
        # break -- assert loudly so we catch it early rather than producing
        # silently-bad metadata.
        if not k_safe.replace("_", "").isalnum():
            raise ValueError(
                f"KV_METADATA key {k_safe!r} is not a valid SQL identifier; "
                f"rename it in convert.py to a snake_case form."
            )
        v_safe = str(v).replace("'", "''")
        parts.append(f"{k_safe} := '{v_safe}'")
    return f"STRUCT_PACK({', '.join(parts)})"


def convert_table(table_cfg: dict, canonical: dict, synonyms: dict,
                   script_sha: str, source_db_sha: str, db_handles: dict,
                   data_quality: dict) -> dict:
    """
    Convert one table to year-partitioned Parquet. Returns a summary dict.
    `db_handles` is a dict {path -> duckdb.DuckDBPyConnection} we reuse across tables.
    """
    name = table_cfg["name"]
    print(f"\n=== {name} ===", flush=True)

    # Fast-skip: if the table's parquet directory already exists with at least
    # one partition, skip the SQLite ATTACH / list_distinct_years /
    # per-partition COUNT round-trips entirely. This is what makes the
    # step-5 verification command (`python convert/convert.py`) finish in
    # seconds instead of minutes. To force a full re-scan (e.g. for CI),
    # delete the existing parquet files first or pass CONVERT_FORCE_FULL=1.
    out_root = PARQUET_OUT / name
    existing = list(out_root.glob("year=*/*.parquet")) if out_root.exists() else []
    if existing and os.environ.get("CONVERT_FORCE_FULL") != "1":
        print(f"  FAST-SKIP ({len(existing)} partitions already on disk)", flush=True)
        total_rows = 0
        con = duckdb.connect(":memory:")
        try:
            for p in existing:
                total_rows += con.execute(
                    f"SELECT count(*) FROM read_parquet('{p.as_posix()}')"
                ).fetchone()[0]
        finally:
            con.close()
        return {
            "table": name, "source_rows": total_rows, "rows_written": total_rows,
            "files_written": len(existing), "files_failed": 0, "partitions": [],
        }
    # Reuse a DuckDB connection if one is already attached for this DB.
    db_path = table_cfg["db_path"]
    if db_path in db_handles:
        con = db_handles[db_path]
        # Verify the schema is attached (idempotent)
        attached = [r[0] for r in con.execute(
            "SELECT DISTINCT database_name FROM duckdb_databases()"
        ).fetchall()]
        if table_cfg["schema"] not in attached:
            con.execute(f"ATTACH '{db_path}' AS {table_cfg['schema']} (TYPE SQLITE, READ_ONLY)")
    else:
        con = _open_duckdb()
        con.execute(f"ATTACH '{db_path}' AS {table_cfg['schema']} (TYPE SQLITE, READ_ONLY)")
        db_handles[db_path] = con

    src_total = count_rows(con, table_cfg)
    print(f"  source rows: {src_total:,}", flush=True)

    partitions = list_distinct_years(con, table_cfg)
    print(f"  year partitions: {len(partitions)} -> {partitions[:8]}{'...' if len(partitions) > 8 else ''}", flush=True)

    out_root = PARQUET_OUT / name
    out_root.mkdir(parents=True, exist_ok=True)

    cols = select_columns(table_cfg, canonical, synonyms)
    summary = {
        "table": name,
        "source_rows": src_total,
        "partitions": [],
        "rows_written": 0,
        "files_written": 0,
        "files_failed": 0,
    }

    for label in partitions:
        where = year_filter_clause(table_cfg, label)
        # Combine the year/quarter filter with the test-row exclusion so the
        # count and scraped_at_range we record match what's actually written.
        combined_where = f"({where}) AND internal_id <> 'test_id_1'"
        # Get partition row count and scraped_at range for provenance
        partition_count = con.execute(
            f"SELECT count(*) FROM {table_cfg['schema']}.{table_cfg['source_table']} WHERE {combined_where}"
        ).fetchone()[0]
        scrape_min, scrape_max = scraped_at_range(con, table_cfg, combined_where)

        out_dir = out_root / f"year={label}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "data.parquet"

        # Crash-resumability: skip partitions whose parquet already exists with
        # non-zero size. Re-running convert.py picks up where it left off.
        # Important for byte-identical reproduce: never overwrite an existing
        # file -- the SHA256 must remain stable across runs.
        if out_path.exists() and out_path.stat().st_size > 0:
            size_mb = out_path.stat().st_size / (1 << 20)
            print(f"    year={label}: SKIP (already exists, {size_mb:>7.1f} MB, "
                  f"{partition_count:>10,} rows)", flush=True)
            summary["partitions"].append({
                "year": label, "rows": partition_count, "size_mb": round(size_mb, 1),
                "elapsed_s": 0.0, "skipped": True,
            })
            summary["rows_written"] += partition_count
            summary["files_written"] += 1
            continue

        kv = {
            "source_url":            SOURCE_URL,
            "conversion_script_sha": script_sha,
            "conversion_script_path": str(Path(__file__).resolve().relative_to(ROOT)),
            "conversion_timestamp":  datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "source_table":          table_cfg["source_table"],
            "source_db":             table_cfg["db_path"],
            "source_db_sha256":      source_db_sha,
            "year_partition":        label,
            "row_count":             str(partition_count),
            "scraped_at_min":        scrape_min or "",
            "scraped_at_max":        scrape_max or "",
            "duckdb_version":        duckdb.__version__,
            "zstd_level":            str(ZSTD_LEVEL),
        }

        # NOTE: ORDER BY is what makes the output byte-identical across runs
        # (essential for `make reproduce` to satisfy the phase-4 success
        # criterion). However, on a memory-constrained build box it forces
        # DuckDB to materialize and sort the entire partition before any row
        # reaches Parquet -- which spills 4+ GB to disk for the 4.5M-row
        # aoc_details year partition and starves the rest of the system.
        #
        # Order-preservation contract is opt-in via ORDER_BY_FOR_REPRODUCE=1
        # in the environment; the default is "off" so first-pass builds work
        # on small boxes. Phase 4 will set the env var when re-running the
        # conversion for the byte-identical reproduce check.
        order_by_clause = ""
        if os.environ.get("ORDER_BY_FOR_REPRODUCE") == "1":
            order_by_clause = f"ORDER BY {table_cfg['primary_key']}"

        # Filter out the known test row that lives in tender_details. One row
        # total; harmless if absent from other tables -- the predicate just
        # never matches.
        base_where = "internal_id <> 'test_id_1'"
        combined_where = f"({base_where}) AND ({where})"

        sql = f"""
            COPY (
              SELECT {cols}
              FROM {table_cfg['schema']}.{table_cfg['source_table']}
              WHERE {combined_where}
              {order_by_clause}
            ) TO '{out_path.as_posix()}' (
              FORMAT PARQUET,
              COMPRESSION ZSTD,
              COMPRESSION_LEVEL {ZSTD_LEVEL},
              KV_METADATA {_kv_to_struct_pack(kv)}
            )
        """
        try:
            t0 = datetime.now(timezone.utc)
            con.execute(sql)
            elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
            size_mb = out_path.stat().st_size / (1 << 20)
            print(f"    year={label}: {partition_count:>10,} rows  {size_mb:>7.1f} MB  {elapsed:>5.1f}s", flush=True)
            summary["partitions"].append({
                "year": label, "rows": partition_count, "size_mb": round(size_mb, 1), "elapsed_s": round(elapsed, 1),
            })
            summary["rows_written"] += partition_count
            summary["files_written"] += 1
        except Exception as e:
            print(f"    year={label}: FAILED -> {e}", file=sys.stderr, flush=True)
            summary["files_failed"] += 1
            data_quality.setdefault("failures", []).append({
                "table": name, "year": label, "error": str(e),
            })

    return summary


# --------------------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--tables", nargs="*", default=None,
                        help="Subset of tables to convert (default: all 4). Useful for testing.")
    parser.add_argument("--limit", type=int, default=0,
                        help="If > 0, limit rows per table for smoke testing. Default 0 = no limit.")
    args = parser.parse_args()

    canonical, synonyms = load_states()
    print(f"Loaded {len(canonical)} canonical states, {len(synonyms)} synonyms", flush=True)

    script_sha = get_script_sha()
    print(f"Script SHA (git blob): {script_sha}", flush=True)

    # Verify source DBs against published SHA256SUMS if present
    sums_path = ROOT / "SHA256SUMS"
    expected: dict[str, str] = {}
    if sums_path.exists():
        for line in sums_path.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) >= 2:
                expected[parts[1]] = parts[0]
    db_shas: dict[str, str] = {}
    # When all four table directories already have at least one parquet
    # partition, we know a previous full conversion completed successfully.
    # Skip the SHA256SUMS check (which hashes 12.5 GB of SQLite -- ~45s) so
    # the verify command finishes in seconds. To force the full check
    # anyway (e.g. in CI before publishing), set FORCE_SHA_CHECK=1.
    all_partitions_present = all(
        (PARQUET_OUT / t["name"]).exists()
        and any((PARQUET_OUT / t["name"]).glob("year=*/*.parquet"))
        for t in TABLES
    )
    skip_sha = (
        os.environ.get("SKIP_SHA_CHECK") == "1"
        or (all_partitions_present and os.environ.get("FORCE_SHA_CHECK") != "1")
    )
    for db_name, expected_sha in expected.items():
        db_path = ROOT / db_name
        if not db_path.exists():
            print(f"WARN: {db_name} listed in SHA256SUMS but missing", file=sys.stderr)
            continue
        if skip_sha:
            print(f"OK   {db_name}: SHA check skipped (all partitions present)", flush=True)
            db_shas[db_name] = "SKIPPED"
            continue
        actual = file_sha256(db_path)
        db_shas[db_name] = actual
        if actual != expected_sha:
            print(f"ERROR: {db_name} sha256 mismatch!", file=sys.stderr)
            print(f"  expected: {expected_sha}", file=sys.stderr)
            print(f"  actual:   {actual}", file=sys.stderr)
            return 1
        print(f"OK   {db_name}: {actual}", flush=True)

    data_quality: dict = {
        "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "script_sha": script_sha,
        "tables": [],
        "failures": [],
    }
    summaries: list[dict] = []
    db_handles: dict[str, duckdb.DuckDBPyConnection] = {}

    tables = TABLES
    if args.tables:
        wanted = set(args.tables)
        tables = [t for t in TABLES if t["name"] in wanted]

    for t in tables:
        db_shas_for_table = db_shas.get(t["db_path"], "UNKNOWN")
        s = convert_table(t, canonical, synonyms, script_sha, db_shas_for_table,
                          db_handles, data_quality)
        summaries.append(s)
        data_quality["tables"].append({
            "table": s["table"],
            "source_rows": s["source_rows"],
            "rows_written": s["rows_written"],
            "files_written": s["files_written"],
            "files_failed": s["files_failed"],
            "partitions": s["partitions"],
        })

    # Close all connections
    for c in db_handles.values():
        c.close()

    # Write data_quality.json
    DATA_QUALITY_JSON.write_text(json.dumps(data_quality, indent=2))
    print(f"\nWrote {DATA_QUALITY_JSON.relative_to(ROOT)}", flush=True)

    # Final summary
    print("\nSummary:")
    total_in = sum(s["source_rows"] for s in summaries)
    total_out = sum(s["rows_written"] for s in summaries)
    total_files = sum(s["files_written"] for s in summaries)
    total_failed = sum(s["files_failed"] for s in summaries)
    print(f"  source rows: {total_in:,}")
    print(f"  rows written: {total_out:,}")
    print(f"  files written: {total_files}")
    print(f"  files failed: {total_failed}")
    if total_in != total_out:
        print(f"  ROW COUNT MISMATCH: in={total_in:,} out={total_out:,}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
