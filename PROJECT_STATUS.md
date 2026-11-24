# Project status

Snapshot of where the project is. Definitions and acceptance criteria live in the spec; this file only points at them.

Last updated: 2026-11-24

## Active

- **Phase:** 0, Foundation & Stabilization (`docs/vision.md`)
- **Milestone:** M0.1, Reproducible core stack + verified Bronze baseline ([spec](.scratch/phase-0/spec.md))

## M0.1 progress

| Step | State |
|---|---|
| 0. Docker reachable from WSL | Passed 2026-11-22: user added to `docker` group; `docker ps` succeeds |
| 1. Core stack up | Passed 2026-11-22 from clean state (`down -v`, then `up -d`): all services running, `hive-metastore` started first time with 0 restarts. Fixes: `HADOOP_OPTIONAL_TOOLS: hadoop-aws`; Postgres `healthcheck` + `condition: service_healthy` for `hive-metastore`, `airflow`, `airflow-scheduler`. Full stack ready in ~165 s |
| 2. Bootstrap | Passed 2026-11-22 from clean state: one-shot `bootstrap` service (`bootstrap/bootstrap.sh`) creates bucket `warehouse` and `iceberg.bronze` at `s3a://warehouse/`; rerun exits 0 (idempotent). `trino_conn` comes from `AIRFLOW_CONN_TRINO_CONN`. Needed a longer Trino healthcheck `start_period` (Trino took ~1 min to start with the full stack) |
| 3. Seed source | Passed 2026-11-22: 9 tables populated (counts in the spec's findings log) |
| 4. DAG run #1 | Passed 2026-11-23 (run `manual__2026-11-23T06:46:28`): 9 `table_checking_*` and 9 `create_table_*` (CTAS) tasks succeeded; `upsert_table_*` skipped as expected. `dbt_ingest_silver` failed with `dbt: command not found` (exit 127), out of scope (M0.2). No S3 errors in Hive Metastore or Trino logs; data files under `s3a://warehouse/`. Peak memory measured on a CTAS rerun (see Memory below) |
| 5. Reconciliation #1 | Passed 2026-11-23 with `scripts/reconcile_bronze.py`, tables listed in `scripts/reconcile_bronze.yaml` (output below). Negative check also run: a wrong PK (`user_id` for `user_addresses`) and a missing table both FAIL with exit code 1 |
| 6. MERGE + idempotency test | Development run passed 2026-11-24 (details in the spec's findings log): reconciliation #2 9/9, 100/100 updates landed, idempotency 9/9 (`_ingested_at` excluded by design). Found and fixed a `MERGE` defect (Bronze-only columns `NULL`/stale); re-verified with runs #4–#5. The official pass is ticket 04 |

Memory (`docker stats`): idle, all 7 services, 2026-11-22: ~2.7 GiB of 9.6 GiB (Trino ~0.9, Airflow webserver ~0.7, scheduler ~0.44, Hive Metastore ~0.45, others < 0.1 each). During a DAG run (2026-11-23, rerun `manual__2026-11-23T09:41:29` after dropping the 9 Bronze tables, so all tables took the CTAS path; ~18 s): peak total **4.99 GiB of 9.6 GiB**, up from 4.34 GiB just before the run. Peaks per service: Trino 2.13, Airflow scheduler 1.19, Airflow webserver 0.98, Hive Metastore 0.61, others < 0.2 each. The scheduler accounts for most of the rise (task processes). Sampled every 4–5 s (5 samples inside the run), so a short spike may have been missed. Trino's idle memory is higher than on 2026-11-22 (2.1 vs ~0.9 GiB) because its JVM keeps heap it has already grown after queries. Reconciliation after this rerun: 9/9 PASS with numbers identical to #1.

Reconciliation #1 output (2026-11-23, `.venv/bin/python scripts/reconcile_bronze.py`, exit code 0):

```
table             src_rows  brz_rows  unique_pk  money_column                         src_sum             brz_sum      diff  result
brands                   8         8          8  -                                          -                   -         -    PASS
categories               5         5          5  -                                          -                   -         -    PASS
products                 8         8          8  base_price                     51,298,000.00       51,298,000.00      0.00    PASS
users                10000     10000      10000  -                                          -                   -         -    PASS
user_addresses       15076     15076      15076  -                                          -                   -         -    PASS
orders               50000     50000      50000  total_amount              929,476,148,000.00  929,476,148,000.00      0.00    PASS
order_items         104854    104854     104854  unit_price_at_purchase    665,335,368,000.00  665,335,368,000.00      0.00    PASS
payments             41726     41726      41726  -                                          -                   -         -    PASS
shipping             41726     41726      41726  shipping_cost               2,400,672,000.00    2,400,672,000.00      0.00    PASS

Sums: diff = brz_sum - src_sum; a sum passes when |diff| <= 1 rupiah.
```

Source sums cross-checked directly in `psql` on `postgres_oltp` (bypassing Trino): identical.

## Findings

- 2026-11-23: restarting an existing stack (start without `down`) fails the `airflow` webserver with `Already running on PID 48 (or pid file '/opt/airflow/airflow-webserver.pid' is stale)`. The pid file survives in the container's writable layer. Workaround: `docker compose up -d --force-recreate airflow`. Permanent fix not decided (needs a `docker-compose.yml` change).

## Next steps

Tickets: `.scratch/phase-0/issues/` (01 resolved; 02, 03, 05 done pending owner review; 06 wontfix). Nothing from 2026-11-24 is committed yet.

1. Owner reviews and commits the 2026-11-24 changes (DAG fixes, `--mode`, spec, `docs/bronze-data-model.md`).
2. Ticket 04: full acceptance run from `docker compose down -v` (destroys all data, needs approval at that point).

## Pending decisions

- Port binding in `docker-compose.yml`: all interfaces vs. `127.0.0.1`.
- Wording of `### Skill precedence` in `CLAUDE.md`.
