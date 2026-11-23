# 01: DAG run #1 + reconciliation script

**What to build:** After a fresh `INIT` seed, the first run of DAG `etl_pipeline` loads all 9 Bronze tables through the CTAS path, and a reconciliation script that lives in the repo proves each Bronze table matches its source. Covers M0.1 steps 4 and 5. The script must be re-runnable later as a regression check.

**Blocked by:** None (can start immediately)

**Status:** resolved

- [x] DAG run #1 completes with every Bronze task successful; all 9 tables took the `create_table` (CTAS) path. `dbt_ingest_silver` is out of scope and may fail.
- [x] Table data is written to `s3a://warehouse/` with no S3 errors in the Hive Metastore or Trino logs (last proof for the `HADOOP_OPTIONAL_TOOLS` fix).
- [x] A reconciliation script is committed to the repo and runs through Trino against both the `postgres` and `iceberg` catalogs.
- [x] For each of the 9 tables it reports: source `count(*)`, Bronze `count(*)`, Bronze `count(DISTINCT <primary key>)`, and, for tables with money columns, source vs. Bronze `sum(<amount>)`.
- [x] It ends with a clear pass/fail per table, using the rules in the spec's "Reconciliation checks" (money sums equal within a small tolerance because of the cast to `DOUBLE`).
- [x] All 9 tables pass, and the run's output is recorded in `PROJECT_STATUS.md`.
- [x] Peak memory during the DAG run is recorded in `PROJECT_STATUS.md`.

## Comments

- 2026-11-23: DAG run #1 and reconciliation #1 passed; output in `PROJECT_STATUS.md`. Script: `scripts/reconcile_bronze.py`, tables in `scripts/reconcile_bronze.yaml` (written and run, not committed yet). Its fail path was checked with a wrong PK, a missing table, and invalid configs. Open: peak memory during a DAG run was not measured; commit pending owner approval.
- 2026-11-23: `.scratch/` was deleted by accident (untracked, so not in git) and rebuilt from Claude Code session transcripts. Rebuilt tickets matched an earlier full read line for line.
- 2026-11-23: Script committed (`b09b755`). For the memory measurement, the 9 Bronze tables were dropped and the DAG rerun (`manual__2026-11-23T09:41:29`), so every table took the CTAS path again; same task results as run #1. Peak total 4.99 GiB of 9.6 GiB (recorded in `PROJECT_STATUS.md`). Reconciliation afterwards: 9/9 PASS, identical numbers. All criteria met.
