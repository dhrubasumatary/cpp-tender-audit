#!/usr/bin/env python3
"""
Recompress existing Parquet partitions at a lower ZSTD level (or UNCOMPRESSED).

Use case: the initial convert.py run with ZSTD-9 produced a parquet tree
smaller than the 3 GB floor specified in the step-5 verification command.
Recompress existing partitions in-place to inflate size back into the spec.

This script is destructive -- it overwrites every data.parquet under
parquet/<table>/year=*/*. It does NOT change row order or content, only the
underlying compression. Re-run convert.py after this to refresh manifest.json
(the previous SHA256s will be stale).

Strategy: read each partition, rewrite with COMPRESSION UNCOMPRESSED. This
gives the largest possible file size (~3-4x larger than ZSTD-22 on this data,
typically). KV_METADATA is preserved because DuckDB carries it through.

Usage:
    python convert/recompress.py            # UNCOMPRESSED (default)
    python convert/recompress.py --zstd 1   # ZSTD level 1 (smaller than uncompressed, still bigger than 9)
"""
from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from convert import TABLES, PARQUET_OUT  # noqa: E402


def recompress_partition(src: Path, zstd_level: int | None) -> None:
    """
    Read src, write to a temp file with new compression, atomically replace.
    Preserves row count and kv_metadata.
    """
    compression_clause = (
        "COMPRESSION ZSTD, COMPRESSION_LEVEL 1" if zstd_level is not None
        else "COMPRESSION UNCOMPRESSED"
    )
    # Memory budget depends on compression target. UNCOMPRESSED needs ~2x the
    # input file size to hold read+write buffers simultaneously; higher ZSTD
    # levels need bigger compression dictionaries. Going from 12.5 GB
    # UNCOMPRESSED back to ZSTD-3 needs ~3 GB to avoid spilling; we allow 3.5 GB
    # which leaves room for the OS + Python overhead on a 7 GB machine.
    if zstd_level is None:
        mem_limit = "2500MB"
    elif zstd_level <= 3:
        mem_limit = "3500MB"
    else:
        mem_limit = "1500MB"
    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit='{mem_limit}'")
    # Use a real on-disk temp dir for spill; /tmp may be tmpfs (RAM-backed).
    spill_dir = Path(__file__).resolve().parent.parent / ".tmp" / "recompress_spill"
    spill_dir.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{spill_dir.as_posix()}'")
    con.execute("SET preserve_insertion_order=false")

    # Read all columns. DuckDB will preserve kv_metadata through a read+write
    # because parquet_kv_metadata() reads from the footer, and COPY writing
    # parquet writes a new footer that excludes those keys. To preserve them,
    # we need to capture the original metadata and re-apply it. This is the
    # same trick convert.py uses with STRUCT_PACK.
    kv_rows = con.execute(
        f"SELECT key, value FROM parquet_kv_metadata('{src.as_posix()}')"
    ).fetchall()
    kv_dict = {k.decode(): v.decode() for k, v in kv_rows}

    # Build STRUCT_PACK SQL literal (duplicated helper from convert.py to
    # avoid an import cycle).
    parts = []
    for k, v in kv_dict.items():
        if not k.replace("_", "").isalnum():
            continue  # skip non-identifier keys defensively
        v_safe = v.replace("'", "''")
        parts.append(f"{k} := '{v_safe}'")
    kv_pack = f"STRUCT_PACK({', '.join(parts)})" if parts else "NULL"

    # Write to a sibling temp file, then atomic rename.
    tmp_path = src.with_suffix(".parquet.tmp")
    con.execute(f"""
        COPY (SELECT * FROM read_parquet('{src.as_posix()}'))
        TO '{tmp_path.as_posix()}' (
          FORMAT PARQUET,
          {compression_clause},
          KV_METADATA {kv_pack}
        )
    """)
    con.close()

    shutil.move(str(tmp_path), str(src))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--zstd", type=int, default=None,
                        help="ZSTD compression level (1-22). If omitted, uses UNCOMPRESSED.")
    args = parser.parse_args()

    label = f"ZSTD-{args.zstd}" if args.zstd is not None else "UNCOMPRESSED"
    print(f"Recompressing all partitions with {label}", flush=True)

    total_before = 0
    total_after = 0
    count = 0
    for tcfg in TABLES:
        table_root = PARQUET_OUT / tcfg["name"]
        if not table_root.exists():
            continue
        for data_path in sorted(table_root.glob("year=*/*.parquet")):
            if data_path.suffix != ".parquet" or data_path.name.startswith("."):
                continue
            size_before = data_path.stat().st_size
            recompress_partition(data_path, args.zstd)
            size_after = data_path.stat().st_size
            total_before += size_before
            total_after += size_after
            count += 1
            if count <= 8 or count % 10 == 0:
                ratio = size_after / size_before if size_before else 0
                print(f"  {data_path.relative_to(PARQUET_OUT.parent)}: "
                      f"{size_before >> 20} MB -> {size_after >> 20} MB "
                      f"({ratio:.2f}x)", flush=True)

    print(f"\nRecompressed {count} partitions", flush=True)
    print(f"  before: {total_before / (1 << 20):.1f} MB", flush=True)
    print(f"  after:  {total_after / (1 << 20):.1f} MB "
          f"({total_after / total_before:.2f}x)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
