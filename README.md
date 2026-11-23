# Medallion Architecture

A local lakehouse that moves data from an operational Postgres database into Bronze, Silver, and Gold layers on Apache Iceberg, orchestrated by Airflow and queried with Trino. The long-term direction is in [docs/vision.md](docs/vision.md).

**Current focus:** Phase 0, stabilizing the existing stack and verifying the Bronze baseline. Progress is tracked in [PROJECT_STATUS.md](PROJECT_STATUS.md).

## Stack

| Component | Role |
|---|---|
| Postgres (`postgres_oltp`) | Operational source database, seeded with synthetic store data |
| MinIO | S3-compatible object storage for the lakehouse (bucket `warehouse`) |
| Hive Metastore | Iceberg catalog, backed by its own Postgres |
| Trino | Query engine over both the source (`postgres` catalog) and the lakehouse (`iceberg` catalog) |
| Airflow | Runs the `etl_pipeline` DAG that loads Bronze |
| dbt | Silver models and snapshots (`dbt_config/`); not yet runnable inside Airflow |

## Run it

```bash
docker compose up -d                              # starts the stack; the bootstrap service creates the bucket and schema
python -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python scripts/initial_data.py          # seeds the source (RUN_MODE = "INIT")
```

Then trigger the `etl_pipeline` DAG in Airflow (http://localhost:8081) and check Bronze against the source:

```bash
.venv/bin/python scripts/reconcile_bronze.py      # exit code 0 = every table matches
```

## Repository layout

| Path | Contents |
|---|---|
| `dags/` | Airflow DAG |
| `scripts/` | Source seed script and the Bronze reconciliation |
| `bootstrap/`, `init/`, `hive-config/`, `trino-config/` | Service configuration |
| `dbt_config/` | dbt project |
| `docs/` | Vision, source data model, and [architecture decisions](docs/adr/) |
| `GLOSSARY.md` | Project vocabulary |
| `.scratch/` | Specs and work tickets |

## How this project is built

I use Claude Code as an assistant: it reviews my designs, explains concepts, and handles minor, repetitive tasks. The direction, design decisions, and trade-offs are mine, and the significant ones are recorded as ADRs in `docs/adr/`. `CLAUDE.md` and `docs/agents/` are the instructions that keep the assistant in that role.
