# CI/CD for the lakehouse repo

**Status:** needs-triage
**Class:** future opportunity (`docs/vision.md` §6). Recorded, not built. Not part of Phase 0 / M0.1.
**Raised:** 2026-11-23, during ticket 01 (reconciliation script design).

## Idea

Run the repo's checks automatically on every push or pull request (CI), and later deploy automatically (CD).

## Why not now

- The repo has no automated tests yet. The reconciliation script from ticket 01 will be one of the first.
- Reconciliation needs the full stack running (Postgres, Trino, MinIO, Hive Metastore; ~2.7 GiB RAM idle, ~165 s startup). Running it in a hosted CI runner is possible but is its own design decision.
- CD has no target: everything runs on one laptop, with no environment to deploy to.

## What keeps the door open

- The reconciliation script exits non-zero when any table fails, so CI can call it unchanged later.

## Open questions for when this is picked up

- Which checks are cheap enough for every push (e.g. DAG import test, `docker compose config` validation, linting) and which need the full stack?
- Hosted runner vs. self-hosted (the laptop) for full-stack checks?
- Which phase does this belong to? Not on the roadmap yet; decide and record as an ADR.
