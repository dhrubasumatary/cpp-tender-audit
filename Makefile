# CPP Tender Audit Tool — build & verification targets.
#
# Conventions:
#   - All conversion runs are deterministic enough that re-running on the same
#     SQLite snapshots produces byte-identical Parquet output (when
#     ORDER_BY_FOR_REPRODUCE=1 is set, which disables the fast-skip path).
#   - First-time build: `make install && make convert && make validate`
#   - CI / sanity check: `make verify`  (runs convert + validate, asserts sizes)

PYTHON ?= python3
DUCKDB_MEMORY_LIMIT ?= 1000MB
DUCKDB_THREADS ?= 2
ZSTD_LEVEL ?= 3
SKIP_SHA_CHECK ?= 0
FORCE_SHA_CHECK ?= 0
ORDER_BY_FOR_REPRODUCE ?= 0

CONVERT_FLAGS = ZSTD_LEVEL=$(ZSTD_LEVEL) DUCKDB_MEMORY_LIMIT=$(DUCKDB_MEMORY_LIMIT) \
                DUCKDB_THREADS=$(DUCKDB_THREADS) SKIP_SHA_CHECK=$(SKIP_SHA_CHECK) \
                FORCE_SHA_CHECK=$(FORCE_SHA_CHECK) \
                ORDER_BY_FOR_REPRODUCE=$(ORDER_BY_FOR_REPRODUCE)

.PHONY: install convert validate manifest reproduce verify clean help

help:
	@echo "Targets:"
	@echo "  install     install Python dependencies (duckdb)"
	@echo "  convert     run convert.py to produce parquet/ tree"
	@echo "  validate    run validate.py to check row counts + JSON round-trip"
	@echo "  manifest    build manifest.json with SHA256s and kv_metadata"
	@echo "  reproduce   byte-identical rebuild (sets ORDER_BY_FOR_REPRODUCE=1)"
	@echo "  verify      sanity check: parquet count >= 50, total size 3-6 GB"
	@echo "  clean       remove parquet/, manifest.json, data_quality.json"

install:
	$(PYTHON) -m pip install --quiet --upgrade pip
	$(PYTHON) -m pip install --quiet duckdb>=1.1.0

convert:
	$(CONVERT_FLAGS) $(PYTHON) convert/convert.py

validate:
	$(PYTHON) convert/validate.py

manifest:
	$(PYTHON) convert/manifest.py

reproduce:
	@echo "=== Byte-identical reproduce: ORDER_BY=1, full re-conversion ==="
	@echo "    Requires deleting parquet/ first (Make target: clean) and"
	@echo "    re-running convert. Each Parquet file's SHA256 should match the"
	@echo "    baseline captured in manifest-baseline.json.sha256."
	rm -rf parquet
	$(CONVERT_FLAGS) ORDER_BY_FOR_REPRODUCE=1 ZSTD_LEVEL=22 $(PYTHON) convert/convert.py
	$(PYTHON) convert/manifest.py
	@if [ -f .reproduce-baseline.sha256 ]; then \
	  diff <(find parquet -name '*.parquet' | sort | xargs sha256sum) \
	       .reproduce-baseline.sha256 && echo OK_BYTE_IDENTICAL; \
	else \
	  echo "Baseline missing. Run 'make capture-baseline' first."; \
	  exit 1; \
	fi

capture-baseline:
	find parquet -name '*.parquet' | sort | xargs sha256sum > .reproduce-baseline.sha256

verify:
	@test -d parquet || (echo "parquet/ missing -- run 'make convert' first"; exit 1)
	@count=$$(find parquet -name '*.parquet' | wc -l); \
	  if [ $$count -lt 50 ] || [ $$count -gt 120 ]; then \
	    echo "FAIL: parquet count $$count not in [50,120]"; exit 1; \
	  else echo "OK parquet count: $$count"; \
	  fi
	@size=$$(du -sh parquet | awk '{print $$1}'); \
	  gb=$$(echo $$size | sed 's/G$$//'); \
	  if [ "$$(echo "$$gb < 3" | bc -l 2>/dev/null || echo 0)" = "1" ] \
	     || [ "$$(echo "$$gb > 6" | bc -l 2>/dev/null || echo 0)" = "1" ]; then \
	    echo "FAIL: parquet size $$size not in [3G,6G]"; exit 1; \
	  else echo "OK parquet size: $$size"; \
	  fi

clean:
	rm -rf parquet manifest.json data_quality.json
