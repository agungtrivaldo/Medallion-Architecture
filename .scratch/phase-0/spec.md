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
| 3 | Seed the source: `scripts/initial_data.py` with `RUN_MODE = "INIT"` | The script succeeds and all 9 source tables in `postgres_oltp` are populated. |
| 4 | Trigger DAG `etl_pipeline` (run #1) | Every Bronze task succeeds; all 9 tables take the `create_table` (CTAS) path. `dbt_ingest_silver` is out of scope and expected to fail. |
| 5 | Reconciliation #1 | All reconciliation checks pass for all 9 tables. |
| 6 | **Under redesign (ticket 02).** Originally: trigger `etl_pipeline` again with no source changes (run #2), then reconciliation #2 must match #1. | — |

Step 6 exists to exercise the `MERGE` path and prove idempotency. As originally written it would pass vacuously (see the 2026-11-22 seed finding in Part 3).

## Reconciliation checks

`scripts/reconcile_bronze.py`, run manually, queries Trino (which reads both the `postgres` and `iceberg` catalogs) and compares each Bronze table with its source table. Tables, primary keys, and money columns are listed in `scripts/reconcile_bronze.yaml`, kept independent of the DAG (ADR 0001):

- source `count(*)` = Bronze `count(*)`
- Bronze `count(DISTINCT <primary key>)` = Bronze `count(*)`
- for tables with money columns, source `sum(<amount>)` ≈ Bronze `sum(<amount>)`, within a small tolerance because Bronze casts these columns to `DOUBLE`

It is an acceptance check and a regression check after DAG changes, not monitoring; automating it is a Phase 3 concern.

## Validity boundary

These checks prove correctness **only** under these conditions:

- the source was seeded fresh by `scripts/initial_data.py`;
- no rows are deleted in the source during the run-through;
- no source row changes more than once between DAG runs.

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
| [04: M0.1 full acceptance run](issues/04-m0-1-acceptance-run.md) | 01, 02, 03 |

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
