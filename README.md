# CPP Tender Audit Tool

A public-interest web application that lets Indian citizens explore and audit
**16.6 million Central Public Procurement Portal (CPPP) tender and award records**
via pre-built investigation queries or custom SQL, filtered by state / year / sector.

The project's credibility rests on three pillars:

1. **Reproducible pipeline.** `make reproduce` regenerates byte-identical Parquet
   output from the source SQLite snapshots (SHA256 match).
2. **Per-file provenance.** Every Parquet file carries 13 key/value metadata
   entries — conversion script SHA, source URL, scrape date, DuckDB version,
   ZSTD level, source-table name — so any downloaded partition is independently
   verifiable.
3. **Zero-loss conversion.** Row counts in Parquet match the source SQLite
   exactly for all 4 tables; JSON round-trip checks on the two detail tables
   return 0 mismatches on 100/100 sampled rows.

---

## Data source

The source SQLite database (12.5 GB, 16.6M records across 4 tables) was
scraped and published by [tender.sarthaksidhant.com](https://tender.sarthaksidhant.com/).
This repository does **not** redistribute the SQLite source — fetch it from
the upstream URL if you want to run `make reproduce` end-to-end.

| Table | Rows | Description |
|---|---:|---|
| `aoc_tenders` | 4,921,960 | Authority of Contract (AoC) award notices |
| `aoc_details` | 4,540,739 | AoC line-item details (JSON blob + flattened columns) |
| `tenders` | 3,952,191 | Live tender notices |
| `tender_details` | 3,178,484 | Tender line-item details (JSON blob + flattened columns) |
| **Total** | **16,593,374** | |

## Parquet dataset

The converted dataset lives in `parquet/` (excluded from git; see below for
download). Structure:

```
parquet/
  aoc_tenders/year=2017/data.parquet
  aoc_tenders/year=2018/data.parquet
  ...
  tender_details/year=unparsed/data.parquet
```

- **84 partition files**, year-partitioned, ZSTD-compressed, 3.0 GB total
  (largest single file: 759 MB)
- **Hybrid JSON storage**: flattened typed columns for query performance +
  `raw_json` escape-hatch column for forward compatibility
- **`state` column** derived from `org_name` / `Location` via a 28-states +
  8-UTs dictionary with 78 synonyms (`convert/states.json`)
- Each file carries 13 metadata entries via DuckDB `kv_metadata`:
  `conversion_script_sha`, `conversion_script_path`, `conversion_timestamp`,
  `duckdb_version`, `row_count`, `scraped_at_min`, `scraped_at_max`,
  `source_db`, `source_db_sha256`, `source_table`, `source_url`,
  `year_partition`, `zstd_level`

`manifest.json` lists every partition with its SHA256, row count, size, and
the full kv_metadata blob, so integrity can be verified with a single
HEAD request per file.

## Quickstart

```bash
git clone https://github.com/dhrubasumatary/cpp-tender-audit.git
cd cpp-tender-audit

# install duckdb python binding
make install

# reproduce the parquet tree from source SQLite (requires the 12.5 GB DB
# files placed at repo root: aoc_tenders.db, tenders_vps.db)
make reproduce

# sanity-check: assert partition count in [50,120] and total size in [3G,6G]
make verify
```

To validate without re-running conversion:

```bash
make validate   # row-count match + 100-row JSON round-trip sample
make manifest   # rebuild manifest.json with fresh SHA256s
```

## Data quality

`data_quality.json` is the canonical validation report. Summary as of the
last run (2026-06-27):

| Table | Row count | PK unique | State populated | JSON round-trip |
|---|---:|:---:|---:|:---:|
| `aoc_tenders` | 4,921,960 ✅ | ✅ | 63.2% | n/a |
| `aoc_details` | 4,540,739 ✅ | ✅ | 12.8% | 100/100 clean |
| `tenders` | 3,952,191 ✅ | ✅ | 6.3% | n/a |
| `tender_details` | 3,178,484 ✅ | ✅ | 19.4% | 100/100 clean |

The low state-population fractions on the `*_details` tables are honest and
intentional: many award line items do not repeat the issuing organisation's
name in their JSON blob, so `state` stays NULL. This is a real data
characteristic, not a conversion bug — do not try to "fix" it.

## Repository layout

```
.
├── convert/
│   ├── convert.py      # SQLite → year-partition Parquet pipeline
│   ├── validate.py      # row-count + JSON round-trip validator
│   ├── manifest.py      # manifest.json builder (SHA256 + kv_metadata)
│   ├── states.json      # 28 states + 8 UTs + 78 synonyms
│   └── recompress.py    # utility: re-ZSTD a parquet tree
├── Makefile             # install / convert / validate / reproduce / verify
├── manifest.json        # per-partition SHA256 + provenance metadata
├── data_quality.json    # canonical validation report
├── LICENSE              # MIT
└── README.md            # this file
```

## Parquet dataset download

The 84 Parquet files (3.0 GB total) are **not** committed to git — they
exceed GitHub's per-file size limits and the architecture is designed for
HTTP range requests against an R2 bucket (the project's next phase).

A read-only mirror is published as GitHub Release assets on the
[`Releases`](../../releases) page. Each partition is a separate asset so you
can download only the years/tables you need. After download, place them
under `parquet/<table>/year=<YYYY>/data.parquet` and run `make verify`.

## Methodology

See `data_quality.json` for the exact validation checks. The conversion
script's git blob SHA is embedded in every Parquet file's kv_metadata under
the key `conversion_script_sha`; the canonical script hash is also listed in
`manifest.json`.

## License

MIT — see [LICENSE](LICENSE). The Parquet dataset is a transformed view of
publicly scraped CPPP records; the upstream source is
[tender.sarthaksidhant.com](https://tender.sarthaksidhant.com/).

## Roadmap

- [x] Phase 1 — SQLite → Parquet pipeline (this repository)
- [ ] Phase 2 — R2 deployment (public bucket, CORS, HTTP range support)
- [ ] Phase 3 — Static site (state picker, flagship audit query cards, SQL editor)
- [ ] Phase 4 — Launch + performance tuning
