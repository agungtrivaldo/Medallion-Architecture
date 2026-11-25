# Project status

Snapshot of where the project is. Definitions and acceptance criteria live in the spec; this file only points at them.

Last updated: 2026-11-25

## Active

- **Phase:** 0, Foundation & Stabilization (`docs/vision.md`)
- **Milestone:** M0.1, Reproducible core stack + verified Bronze baseline ([spec](.scratch/phase-0/spec.md)). **Acceptance run (ticket 04) passed 2026-11-25; awaiting owner sign-off to mark M0.1 done.**

## M0.1 acceptance run (ticket 04), 2026-11-25

One uninterrupted run from `docker compose down -v`, no edits to tracked files during the run. Step 4 uses the unpause procedure agreed on 2026-11-25 (spec Part 3).

| Step | Result |
|---|---|
| 0. Docker reachable | PASS: `docker ps` succeeds |
| 1. Stack up | PASS: all services running after `up -d`. Memory just after start, idle: ~2.2 GiB of 9.6 GiB (Trino 1.0, Hive Metastore 0.59, others < 0.2; Airflow still starting) |
| 2. Bootstrap | PASS: `bootstrap` exited 0; `SHOW SCHEMAS FROM iceberg` lists `bronze` |
| 3. Seed `--mode init` | PASS: 9 tables populated (users 10,000; user_addresses 15,003; orders 50,000; order_items 105,041; payments/shipping 41,629; products 8; brands 8; categories 5). Counts differ from 2026-11-22 because the seed is random |
| 4. Unpause, run #1 | PASS: DAG was paused after `up` (as expected); `airflow dags unpause` made the scheduler create `scheduled__2026-11-24T00:00:00+00:00`. 9 `table_checking` + 9 `create_table` succeeded, 9 `upsert_table` skipped, `dbt_ingest_silver` failed (expected, M0.2) |
| 5. Reconciliation #1 | PASS 9/9, exit 0 |
| 6a. Seed `--mode incremental` | PASS: 50 users, 200 orders added, 100 orders updated |
| 6b. Run #2 | PASS: `manual__2026-11-25T04:00:57+00:00`, `data_interval_start` 2026-11-24 00:00 UTC. 6 `upsert_table` (MERGE), 3 `create_table` |
| 6c. Validity guard | VALID: 200 `ORD-INCR-` (INSERT branch), 100 `ORD-INIT-` (UPDATE branch) |
| 6d. Reconciliation #2 | PASS 9/9, exit 0 (users 10,050; user_addresses 15,074; orders 50,200; order_items 105,467; payments/shipping 41,796). Extra: 0 `NULL` `_ingested_at`/`_source_system`/`updated_at` in the 6 MERGE tables (2026-11-24 defect did not return) |
| 6e. Capture | Done: 9 snapshot IDs saved |
| 6f. Run #3 | PASS: `manual__2026-11-25T04:12:43+00:00`, same paths as 6b |
| 6g. Idempotency | PASS 9/9: counts equal, unique PK, two-way `EXCEPT` 0/0 (`_ingested_at` excluded). Negative check: with `_ingested_at` included, `orders` differs on 300 rows, as expected |
| 7. Pause | PASS: `airflow dags list` shows `etl_pipeline` paused |

Peak memory across the run (`docker stats` every ~4 s, 231 samples, 10:57–11:14 WIB): **4.90 GiB of 9.6 GiB** total, during run #2. Per-service peaks: Trino 2.15, Airflow scheduler 1.15, Airflow webserver 0.92, Hive Metastore 0.75, others < 0.2 each.

Earlier per-step development results (2026-11-22 to 2026-11-24) are in the spec's findings log.

## Findings

- 2026-11-23: restarting an existing stack (start without `down`) fails the `airflow` webserver with `Already running on PID 48 (or pid file '/opt/airflow/airflow-webserver.pid' is stale)`. The pid file survives in the container's writable layer. Fixed 2026-11-25 (owner-approved `docker-compose.yml` change): the `airflow` command runs `rm -f /opt/airflow/airflow-webserver.pid` before `exec airflow webserver`. Validated: with the old command, a pid file holding a live PID (`1`) reproduced the exact error and exit 1; with the fix, a fresh container (no pid file), the same forced stale pid file, and a plain `stop`/`start` all reached `/health` 200 with no `Already running` error. A normal `stop`/`start` on 2026-11-25 had worked even before the fix, which fits the failure depending on whether the old PID happens to be in use (assumption).
- 2026-11-25: input for M0.2. About 87 s of each Airflow start is `pip install` of `_PIP_ADDITIONAL_REQUIREMENTS` (from the container log), repeated on every start. M0.2 must add dbt to the Airflow image anyway; a custom image would remove both. Not done now.

## Next steps

Tickets: `.scratch/phase-0/issues/` (01 resolved; 02, 03, 04, 05 done pending owner review; 06 wontfix). The 2026-11-25 changes (spec step 4/7, this file, ticket 04) are not committed yet.

1. Owner reviews the acceptance results, closes tickets 02–05, and signs off M0.1.
2. Then plan M0.2 (dbt / Silver).

## Pending decisions

- Port binding in `docker-compose.yml`: all interfaces vs. `127.0.0.1`.
- Wording of `### Skill precedence` in `CLAUDE.md`.
