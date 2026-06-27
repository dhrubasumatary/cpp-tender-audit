#!/usr/bin/env python3
"""
Build manifest.json for the Parquet output of convert.py.

Walks parquet/<table>/year=<label>/data.parquet, and for each file records:
    path        -- relative path under repo root
    sha256      -- SHA256 of the file's bytes (matches what we'll upload to R2)
    rows        -- exact row count
    table       -- source SQLite table name
    year        -- partition label (e.g. "2017", "unparsed", "2026-q2")
    kv_metadata -- the provenance blob stored in the Parquet footer

Manifest.json is the source of truth that R2 uploads verify against:
every partition uploaded to the bucket must have a SHA256 that matches the
manifest entry, and a HEAD against the R2 object must return the same ETag.

Usage:
    python convert/manifest.py            # writes manifest.json
    python convert/manifest.py --out path # custom output path
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

# Reuse configuration from convert.py (single source of truth).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from convert import (  # noqa: E402
    TABLES,
    ROOT,
    PARQUET_OUT,
    SOURCE_URL,
)


def _file_sha256(path: Path) -> str:
    """Stream SHA256 so we don't load the whole Parquet into memory."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _partition_label_from_path(parquet_path: Path) -> str:
    """year=2017 -> '2017'; year=2026-q2 -> '2026-q2'; year=unparsed -> 'unparsed'."""
    parent = parquet_path.parent.name
    if not parent.startswith("year="):
        raise ValueError(f"Unexpected partition dir name: {parent}")
    return parent[len("year="):]


def _read_kv_metadata(parquet_path: Path) -> dict:
    """Read the KV_METADATA footer from a Parquet file. Returns {} on error."""
    try:
        # NOTE: do NOT use read_only=True here -- parquet files aren't DuckDB
        # databases, and opening one as read_only raises "Cannot launch
        # in-memory database in read-only mode". An in-memory DuckDB can
        # query any parquet path via the parquet_kv_metadata() table function.
        con = duckdb.connect(":memory:")
        try:
            rows = con.execute(
                f"SELECT key, value FROM parquet_kv_metadata('{parquet_path.as_posix()}')"
            ).fetchall()
        finally:
            con.close()
        # DuckDB returns BYTES; decode to str.
        out: dict = {}
        for k, v in rows:
            try:
                k_str = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
                v_str = v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
            except Exception:
                k_str, v_str = str(k), str(v)
            out[k_str] = v_str
        return out
    except Exception as e:
        return {"_read_error": str(e)}


def build_manifest() -> dict:
    """Walk parquet/ and assemble the manifest dict."""
    con = duckdb.connect(":memory:")
    partitions: list[dict] = []

    for tcfg in TABLES:
        table_root = PARQUET_OUT / tcfg["name"]
        if not table_root.exists():
            continue

        for year_dir in sorted(table_root.glob("year=*")):
            if not year_dir.is_dir():
                continue
            data_path = year_dir / "data.parquet"
            if not data_path.exists():
                continue

            label = _partition_label_from_path(data_path)
            sha = _file_sha256(data_path)
            row_count = con.execute(
                f"SELECT count(*) FROM read_parquet('{data_path.as_posix()}')"
            ).fetchone()[0]
            kv = _read_kv_metadata(data_path)

            partitions.append({
                "path":          str(data_path.relative_to(ROOT)),
                "table":         tcfg["source_table"],
                "year":          label,
                "rows":          int(row_count),
                "size_bytes":    data_path.stat().st_size,
                "sha256":        sha,
                "kv_metadata":   kv,
            })

    con.close()

    # Cover the verification's "table" key: the original schema used
    # `source_table` as the human-readable name; include both so consumers can
    # pick whichever matches their vocabulary.
    for p in partitions:
        p.setdefault("source_table", p["table"])

    manifest = {
        "as_of":           datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source_url":      SOURCE_URL,
        "parquet_root":    PARQUET_OUT.relative_to(ROOT).as_posix(),
        "partition_count": len(partitions),
        "total_rows":      sum(p["rows"] for p in partitions),
        "total_size_bytes": sum(p["size_bytes"] for p in partitions),
        "tables":          sorted({p["table"] for p in partitions}),
        "partitions":      partitions,
    }
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--out", type=str, default=str(ROOT / "manifest.json"),
                        help="Path for manifest.json output (default: ./manifest.json)")
    args = parser.parse_args()

    print(f"Building manifest from {PARQUET_OUT.relative_to(ROOT)}/", flush=True)
    manifest = build_manifest()
    out_path = Path(args.out)
    out_path.write_text(json.dumps(manifest, indent=2))

    print(f"Wrote {out_path.relative_to(ROOT) if out_path.is_absolute() else out_path} "
          f"({manifest['partition_count']} partitions, "
          f"{manifest['total_rows']:,} rows, "
          f"{manifest['total_size_bytes'] / (1 << 20):.1f} MB total)",
          flush=True)
    for tbl in manifest["tables"]:
        n = sum(1 for p in manifest["partitions"] if p["table"] == tbl)
        rows = sum(p["rows"] for p in manifest["partitions"] if p["table"] == tbl)
        print(f"  {tbl}: {n} partitions, {rows:,} rows", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
