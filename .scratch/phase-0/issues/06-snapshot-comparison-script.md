# 06: Snapshot comparison script (`capture` / `compare`)

**What to build:** A script that proves a DAG rerun with no source change left each Bronze table's content unchanged, as defined in the spec's Idempotency check. `capture` saves the current Iceberg snapshot ID of each of the 9 Bronze tables to a file; `compare` reads that file and, per table, checks the row count against the captured snapshot, `count(*)` = `count(DISTINCT pk)`, and a two-way `EXCEPT` between the captured snapshot (`FOR VERSION AS OF`) and the current table. It queries through Trino like `scripts/reconcile_bronze.py`, and takes tables and primary keys from `scripts/reconcile_bronze.yaml` rather than from the DAG (ADR 0001). Design decided in ticket 02.

**Blocked by:** 02

**Status:** wontfix

- [ ] Verified on the running stack before relying on it: Trino's `"<table>$snapshots"` metadata table and `FOR VERSION AS OF <snapshot_id>` work on the Bronze tables.
- [ ] Verified: time travel to a snapshot taken before a `CREATE OR REPLACE TABLE` still works, since `brands`, `categories`, and `products` take CTAS on every run. If it does not, the design for those 3 tables is brought back to the owner before writing more code.
- [ ] Recorded: whether a `MERGE` that rewrites identical values commits a new snapshot (the spec currently states this as an assumption).
- [ ] `capture` writes a file mapping each of the 9 tables to its current snapshot ID; `compare` fails clearly if that file is missing.
- [ ] `compare` prints one result line per table and exits 1 if any table fails, 0 if all pass.
- [ ] Negative checks, run and recorded: a row changed in Bronze between `capture` and `compare` fails the `EXCEPT` check, and a duplicated row fails the count checks. The test changes are undone afterwards.
- [ ] Where the snapshot file lives, and whether it is git-ignored, is decided with the owner.
- [ ] No new dependency beyond what `scripts/reconcile_bronze.py` already uses.

## Comments

2026-11-24: cancelled by the owner to cut scope. The idempotency check runs as manual Trino queries written in the spec's Idempotency check section. Revisit as a script only if the check is needed repeatedly (for example after Bronze is rebuilt on CDC).
