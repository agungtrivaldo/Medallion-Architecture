# Phase 0 spec: Foundation & Stabilization

Status: needs-triage

Phase goal (from `docs/vision.md` §5): the existing PostgreSQL → Bronze → Silver pipeline runs reproducibly from this repository, and its correctness is checked rather than assumed.

Phase 0 is split into milestones. This spec defines **M0.1**. Later milestones are listed near the end and are not yet specified.

How to read this file: **Part 1 is the contract** (what "done" means; it changes only by an explicit owner decision). **Part 2 is the work left.** **Part 3 is the findings log** (what we learned along the way, dated, append-only).

---

# Part 1: M0.1 contract

**M0.1: Reproducible core stack + verified Bronze baseline.**

M0.1 verifies the Bronze pipeline **as it is implemented today**. It does not change Bronze's design; the gaps between the implementation and the target definition of Bronze (see `GLOSSARY.md`) are listed under Known deviations and belong to Phase 1.

## Acceptance workflow

M0.1 is done when every step passes, in order, on a single run-through from a clean state (`docker compose down -v`).

| Step | Action | Passes when |
|---|---|---|
| 0 | Environment prerequisite | `docker ps` succeeds from the WSL shell. |
| 1 | Start the stack: `docker compose up -d` | All services run with no manual edits to tracked files. Memory from `docker stats` is recorded in `PROJECT_STATUS.md`. |
| 2 | Bootstrap (runs automatically as the one-shot `bootstrap` service) | `bootstrap` exits 0, and `SHOW SCHEMAS FROM iceberg` lists `bronze`. |
| 3 | Seed the source: `python scripts/initial_data.py --mode init` | The script succeeds and all 9 source tables in `postgres_oltp` are populated. |
| 4 | Trigger DAG `etl_pipeline` (run #1) | Every Bronze task succeeds; all 9 tables take the `create_table` (CTAS) path. `dbt_ingest_silver` is out of scope and expected to fail. |
| 5 | Reconciliation #1 | All reconciliation checks pass for all 9 tables. |
| 6a | Change the source: `python scripts/initial_data.py --mode incremental` | The script succeeds. |
| 6b | Trigger `etl_pipeline` (run #2) | Every Bronze task succeeds. The 6 tables with an `updated_at` watermark (`users`, `user_addresses`, `orders`, `order_items`, `payments`, `shipping`) take the `upsert_table` (`MERGE`) path. `brands`, `categories`, and `products` have no watermark and take CTAS again. |
| 6c | Validity guard for run #2 | Read run #2's `data_interval_start` from the output of `airflow dags trigger etl_pipeline` or from the run's details in the UI (`airflow dags list-runs -o json` does not include it in Airflow 2.9.1). The DAG filters on it as a UTC timestamp without offset; the guard uses the same value. In the source, `orders` rows with `updated_at >=` that value include at least one `ORD-INCR-%` order (exercises the `INSERT` branch) and at least one `ORD-INIT-%` order (exercises the `UPDATE` branch). If either count is 0, step 6 is **invalid**: it does not pass, and 6a–6b are repeated after the cause is fixed. |
| 6d | Reconciliation #2 | All reconciliation checks pass for all 9 tables, an exact match with the changed source. Proves that new and updated rows reached Bronze. |
| 6e | Capture (manual Trino queries, see Idempotency check) | The current Iceberg snapshot ID of each of the 9 Bronze tables is saved to a file. |
| 6f | Trigger `etl_pipeline` again with no source change in between (run #3) | Every Bronze task succeeds, on the same paths as 6b. |
| 6g | Idempotency check (manual Trino queries, see Idempotency check) | Passes for all 9 tables (see Idempotency check). |

Step 6 proves two separate things: `MERGE` correctness (6a–6d: new and updated source rows land in Bronze) and idempotency (6e–6g: a rerun with no source change leaves Bronze's content unchanged). The original step 6, a rerun straight after the `INIT` seed, would pass vacuously because no `INIT` row falls inside the `MERGE` filter (see the 2026-11-22 seed finding in Part 3); step 6c exists so that no check can pass on zero rows.

## Reconciliation checks

`scripts/reconcile_bronze.py`, run manually, queries Trino (which reads both the `postgres` and `iceberg` catalogs) and compares each Bronze table with its source table. Tables, primary keys, and money columns are listed in `scripts/reconcile_bronze.yaml`, kept independent of the DAG (ADR 0001):

- source `count(*)` = Bronze `count(*)`
- Bronze `count(DISTINCT <primary key>)` = Bronze `count(*)`
- for tables with money columns, source `sum(<amount>)` ≈ Bronze `sum(<amount>)`, within a small tolerance because Bronze casts these columns to `DOUBLE`

It is an acceptance check and a regression check after DAG changes, not monitoring; automating it is a Phase 3 concern.

## Idempotency check

Manual Trino queries compare each Bronze table with its own earlier version, using Iceberg time travel (ticket 06, a script, was cancelled to cut scope). Capture: for each of the 9 tables, record `SELECT snapshot_id FROM iceberg.bronze."<table>$snapshots" ORDER BY committed_at DESC LIMIT 1`. Compare, per table:

- Bronze `count(*)` now = Bronze `count(*)` at the captured snapshot
- Bronze `count(DISTINCT <primary key>)` = Bronze `count(*)`
- `SELECT <all columns except _ingested_at> FROM <table> FOR VERSION AS OF <captured snapshot>` `EXCEPT` the same columns from the current table returns 0 rows, and so does the reverse direction

`_ingested_at` is left out because it records when the pipeline wrote the row, not source data: CTAS re-stamps it on every run and `MERGE` re-stamps every row inside the filter window, so it changes on a correct rerun. Every other column, including `updated_at`, must stay identical. "Unchanged" means unchanged **content**, not "no new snapshot": a `MERGE` that rewrites identical values may still commit a snapshot (fact, 2026-11-24: run #3 added one snapshot to every table). `EXCEPT` compares rows as a set and does not see duplicates, which is why the two count checks stay. This is not a rolling baseline: only the acceptance workflow knows whether the source changed between two runs, so it decides when to capture and when to compare.

The contract (a rerun with no source change leaves Bronze's content unchanged) does not depend on the tool; the mechanism (Iceberg snapshot IDs and time travel) does, and is accepted as such.

## Validity boundary

These checks prove correctness **only** under these conditions:

- the source was seeded fresh by `scripts/initial_data.py --mode init`, and changed afterwards only by `--mode incremental`;
- no rows are deleted in the source during the run-through;
- no source row changes more than once between DAG runs.

`--mode incremental` meets the last two conditions: it inserts new rows and updates each changed order once, and issues no `DELETE`. That is why reconciliation #2 still demands an exact match with the source. If the source did delete rows, Bronze would keep them (the `MERGE` has no delete branch, and its source query never sees a deleted row), so `brz_rows > src_rows` by the number of deleted rows would be expected, not a bug.

The batch pipeline cannot capture deletes or intermediate changes, so these conditions are deliberate. Proving correctness without them is Phase 1 work.

## Known deviations from the target Bronze definition

Accepted for M0.1; fixed in Phase 1. Listed so nobody mistakes them for intended design.

- **Overwrite instead of append.** `upsert_table` uses `MERGE … WHEN MATCHED THEN UPDATE`, so earlier versions of a row are lost.
- **Deletes are not captured.** A row deleted in the source stays in Bronze with no delete event.
- **Joins in Bronze.** `order_items`, `payments`, and `shipping` join `orders`, and `user_addresses` joins `users`, to borrow `updated_at` as a watermark. A change to a child row that does not touch its parent is missed.
- **Lossy type casts.** Money columns (`orders.total_amount`, `order_items.unit_price_at_purchase`, `products.base_price`, `shipping.shipping_cost`) are cast to `DOUBLE`, which can lose precision.
- **Capture granularity.** A daily batch only sees each row's state at run time; intermediate changes within a day are lost.

## Out of scope for M0.1

- The dbt / Silver task (`dbt_ingest_silver`), including the missing `dbt_project.yml`, `profiles.yml`, sources file, dbt not being installed in the Airflow images, and the dbt volume mount that points at an absolute host path outside the repo. These belong to M0.2.
- Any change to Bronze's design (see Known deviations).
- Moving credentials out of tracked files (see the connection config refactor milestone).

---

# Part 2: Work left for M0.1

Steps 0–3 passed on 2026-11-22. The remaining work is tracked as tickets (one file each; details and acceptance checklists live there, not here):

| Ticket | Blocked by |
|---|---|
| [01: DAG run #1 + reconciliation script](issues/01-dag-run-1-and-reconciliation.md) | — |
| [02: Redesign step 6 (MERGE + idempotency test)](issues/02-redesign-step-6.md) | — (recommended after 01) |
| [03: Fix the `user_addresses` merge key](issues/03-fix-user-addresses-merge-key.md) | 01 |
| [04: M0.1 full acceptance run](issues/04-m0-1-acceptance-run.md) | 01, 02, 03, 05 |
| [05: Seed mode as a required command-line argument](issues/05-seed-mode-argument.md) | 02 |
| [06: Snapshot comparison script (`capture` / `compare`)](issues/06-snapshot-comparison-script.md), wontfix | 02 |

Context for 02: `scripts/initial_data.py` already has a `RUN_MODE = "INCREMENTAL"` change simulator (50 new users, 200 new orders, up to 100 `pending` orders updated to `Completed` with `updated_at = NOW()`).

---

# Later milestones (not yet specified)

- **M0.2:** make the dbt / Silver snapshot task run, and reconcile Silver against Bronze.
- **Connection config refactor** (agreed 2026-11-22; order relative to M0.2 not decided): move every credential into a git-ignored `.env`, commit a `.env.example` with dummy values, have `docker-compose.yml` use `${...}` substitution, and have host scripts such as `scripts/initial_data.py` read `os.environ`, loading `.env` with `python-dotenv` on the host. Done after M0.1 so the M0.1 reconciliation script can prove the refactor changed no data. Adds a dependency (`python-dotenv`), which needs approval when the milestone starts.

---

# Part 3: Findings log

Append-only. Newest last.

### 2026-11-22: Docker access (step 0)

The Docker socket is `root:docker` with mode `660`, and user `agung` was not in the `docker` group. Fixed by adding the user to the group and restarting WSL. Note: in bash, `wsl` resolves to an unrelated tool (`/usr/bin/wsl`, wsmancli); use `wsl.exe --shutdown`.

### 2026-11-22: Old containers inspected, then removed

Read-only inspection of the containers left from the earlier manual setup recovered the bootstrap facts:

- MinIO held buckets `warehouse` and `test`.
- Metastore schema `bronze` had location `s3a://warehouse/`, with its 9 tables under `warehouse/<table>-<uuid>/`. Schema `test` (`s3a://test/`) looked like a leftover experiment.
- Airflow connection `trino_conn`: type `trino`, host `trino`, port `8080`, login `admin`, no password, non-empty encrypted `extra` (not read).

The old containers, their three anonymous volumes, and the orphaned `jupyter` container were then removed with owner approval.

### 2026-11-22: Hive Metastore S3 jars were hand-installed

`docker diff` showed `hadoop-aws-3.3.6.jar` and `aws-java-sdk-bundle-1.12.723.jar` copied by hand into `/opt/hive/lib/` of the old `hive-metastore` container. The image already ships `hadoop-aws-3.3.6.jar` and `aws-java-sdk-bundle-1.12.367.jar` in `/opt/hadoop/share/hadoop/tools/lib/`, off the classpath. Fix: `HADOOP_OPTIONAL_TOOLS: hadoop-aws` on `hive-metastore`. Throwaway-container test: the setting puts both jars on the metastore classpath, and `s3a://` write/read/list against a throwaway MinIO succeeded; without it, `ClassNotFoundException: S3AFileSystem`. Step 2 then created `iceberg.bronze` at `s3a://warehouse/` with no S3 errors. The first table-data write (step 4) is the last proof.

### 2026-11-22: Startup race with Postgres (step 1)

On a fresh volume Postgres runs `initdb` and `init.sql` on a temporary socket-only server before the real one accepts TCP connections. `hive-metastore` connected too early and exited 1. Fix: a Postgres `healthcheck` (`pg_isready -h 127.0.0.1`, which forces TCP) and `condition: service_healthy` on `hive-metastore`, `airflow`, and `airflow-scheduler`. Clean-state test: metastore started first time, 0 restarts.

### 2026-11-22: Bootstrap automated (step 2)

A one-shot `bootstrap` service (the Trino image, used only for its `curl` and `trino` CLI) runs `bootstrap/bootstrap.sh`: creates bucket `warehouse` (HTTP 409 = already exists = fine) and runs `CREATE SCHEMA IF NOT EXISTS iceberg.bronze WITH (location = 's3a://warehouse/')`. MinIO credentials reach it through a YAML anchor on the `minio` service's environment. `trino_conn` is defined by `AIRFLOW_CONN_TRINO_CONN` on both Airflow services. A rerun exits 0 (idempotent).

### 2026-11-22: Trino healthcheck too strict (step 2)

With the whole stack starting at once, Trino took about 1 minute to start. The image's built-in healthcheck (`start_period` 10 s) marked it unhealthy first, so Compose never started `bootstrap`. Fix: the same check with `start_period: 180s`, `retries: 6`. `start_period` is a grace window, not a fixed wait: `bootstrap` starts as soon as Trino passes.

### 2026-11-22: Memory, idle

All 7 services idle: about 2.7 GiB of 9.6 GiB (Trino about 0.9, Airflow webserver about 0.7, scheduler about 0.44, Hive Metastore about 0.45, the rest under 0.1 each).

### 2026-11-22: Seed results (step 3)

`users` 10,000; `user_addresses` 15,076; `orders` 50,000; `order_items` 104,854; `payments` 41,726; `shipping` 41,726; `products` 8; `brands` 8; `categories` 5. `users.updated_at` and `orders.updated_at` range from 2025-01-01 to 2026-03-27; `products.updated_at` is the seed time. 5,076 users have more than one address. These dates are why the original step 6 would pass vacuously (Part 2, item 3).

### 2026-11-24: Step 6 redesigned (ticket 02)

Facts found while designing the new step 6:

- `brands`, `categories`, and `products` have an empty `watermark` in the DAG's `TABLE` config, so `table_checking` sends them to CTAS (`CREATE OR REPLACE TABLE`) on every run. Only the other 6 tables ever take the `MERGE` path.
- The task logs of earlier runs record `AIRFLOW_CTX_EXECUTION_DATE` but not `data_interval_start`, and the DAG does not log the `run_date` it filters on. The validity guard (step 6c) therefore reads the data interval from Airflow's run metadata after the run.
- `--mode init` runs `DROP TABLE ... CASCADE` on all 9 source tables and still prints `SUCCESS!`. This is why the seed mode becomes a required argument with no default (ticket 05) instead of a hand edit.

### 2026-11-24: First run of the redesigned step 6 (development run, not the acceptance run)

Started from the step 5 state (pre-check reconciliation 9/9 PASS), after tickets 03 and 05.

- 6a: `--mode incremental` added 50 users, 67 addresses, 200 `ORD-INCR-` orders, and updated 100 `pending` orders. Source timestamps mix time zones: inserted rows use the host's local time (`datetime.now()`, WSL at UTC+7), updated rows use Postgres `NOW()` (UTC); some new orders have `updated_at` up to 2026-11-25, in the future.
- 6b: run #2 `manual__2026-11-24T07:31:54+00:00`, `data_interval_start` 2026-11-23 00:00 UTC (from the trigger output; confirms that a manual `@daily` run starts at the previous midnight UTC). All Bronze tasks succeeded: 6 `upsert_table`, 3 `create_table`; `dbt_ingest_silver` failed as expected.
- 6c: rows passing the filter: 200 `ORD-INCR-` (INSERT branch), 100 `ORD-INIT-` (UPDATE branch). Valid.
- 6d: reconciliation #2 9/9 PASS (`users` 10,050, `user_addresses` 15,143, `orders` 50,200, `order_items` 105,259, `payments`/`shipping` 41,887). Extra check: all 100 updated orders have the source's new `order_status` in Bronze.
- 6e–6g: run #3 `manual__2026-11-24T07:33:54+00:00`, same paths. The 6 `MERGE` tables PASS (counts equal, unique PK, two-way `EXCEPT` 0/0). The 3 CTAS tables (`brands`, `categories`, `products`) FAIL on every row; with `_ingested_at` excluded, `EXCEPT` is 0/0. Cause: the CTAS query re-stamps `current_timestamp AS _ingested_at` on each full rewrite. `CREATE OR REPLACE TABLE` keeps snapshot history, so time travel works on these tables.
- Defect found: every row inserted through the `MERGE` `INSERT` branch has `NULL` `_ingested_at` and `_source_system` (users 50, user_addresses 67, orders 200, order_items 405, payments 161, shipping 161, exactly the new rows), and `user_addresses` also gets `NULL` `created_at`/`updated_at`. `upsert_table` builds its column list from the source table's `information_schema`, so Bronze-only columns (metadata and joined columns) are never written. Reconciliation (counts, PK, sums) cannot see this.
- Same defect, second effect: `order_items`, `payments`, and `shipping` borrow `updated_at` from `orders`, and the `MERGE` `UPDATE` branch never refreshes it. After 100 orders changed, the matching 100 `shipping` rows kept the orders' old `updated_at`; new child rows have `NULL` `updated_at` (order_items 405, payments 161, shipping 161). Bronze schema reference: `docs/bronze-data-model.md`.

### 2026-11-24: `MERGE` column-list defect fixed and re-verified

With owner approval, `upsert_table` now takes its column list from the Bronze table (`iceberg.information_schema.columns`, schema `bronze`, ordered by `ordinal_position`) instead of the source table, so joined and metadata columns are written on both `INSERT` and `UPDATE`.

- Run #4 `manual__2026-11-24T08:15:50+00:00` (no source change; the broken rows were still inside the filter window, so `MERGE` rewrote them). Afterwards, across the 6 `MERGE` tables: 0 rows with `NULL` `_ingested_at`, `_source_system`, or `updated_at`; 0 child rows (`order_items`, `payments`, `shipping`) whose `updated_at` differs from their order's; 0 `user_addresses` rows whose `created_at`/`updated_at` differ from their user's. Reconciliation 9/9 PASS.
- Idempotency re-run: capture after run #4, run #5 `manual__2026-11-24T08:27:05+00:00`, compare with `_ingested_at` excluded: 9/9 PASS (counts equal, unique PK, two-way `EXCEPT` 0/0). With `_ingested_at` included, `orders` differs on 300 rows (200 `ORD-INCR-` + 100 updated, the rows inside the window), as expected.
