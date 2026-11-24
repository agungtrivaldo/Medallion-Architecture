# 02: Redesign M0.1 step 6 (MERGE + idempotency test)

**What to build:** A decision, proposed by the owner, on how step 6 proves two separate things: (a) idempotency, meaning a DAG rerun with no source changes leaves Bronze unchanged, and (b) `MERGE` correctness, meaning new and updated source rows land in Bronze. The original step 6 would pass vacuously: the `INIT` seed's `updated_at` values all fall before 2026-03-28, so a `MERGE` filtered on `data_interval_start` receives zero rows. The decision is written into the spec's contract as the new step 6.

**Blocked by:** None (can start immediately). Recommended after 01, so the owner has seen a real DAG run first.

**Status:** needs-triage

- [x] The owner proposes the order of DAG runs and `INCREMENTAL` seeds, and it is evaluated against both goals (a) and (b).
- [x] The design states what each DAG run must show, including which tables are expected to take the `MERGE` path and which take CTAS.
- [x] The design checks that the `INCREMENTAL` seed's changes actually reach the `MERGE` filter (rows with `updated_at` inside the run's data interval), so no check passes with zero rows.
- [x] Decided: whether `RUN_MODE` stays a hand edit in the seed script or becomes a command-line argument.
- [x] Decided: how reconciliation is interpreted after an `INCREMENTAL` seed (the validity boundary still excludes deletes and multiple changes per row between runs).
- [x] The spec's step 6 and validity boundary are updated with the agreed design.

## Design notes (in progress, 2026-11-24)

Agreed with the owner so far; not yet written into the spec.

Run order:

1. `INCREMENTAL` seed.
2. DAG run #2: the 6 tables with an `updated_at` watermark take the `MERGE` path; `brands`, `categories`, and `products` have no watermark and take CTAS on every run (corrected 2026-11-24 after reading `table_checking`).
3. Validity guard for run #2: read the run's real `data_interval_start` (Airflow UI or `airflow dags list-runs -d etl_pipeline -o json`), then count source rows that pass the DAG's filter (`updated_at >= data_interval_start`). Zero rows makes step 6 **invalid**, not passed. Both `MERGE` branches need rows: new orders (`order_id` prefix `ORD-INCR-`) for `INSERT`, and `ORD-INIT-` orders whose `updated_at` is inside the interval for `UPDATE`.
4. Reconciliation (`scripts/reconcile_bronze.py`): proves goal (b).
5. `capture`: save each Bronze table's current Iceberg snapshot ID to a file.
6. DAG run #3 with no seed in between.
7. `compare`: per table, row count unchanged, `count(*)` = `count(DISTINCT pk)`, and two-way `EXCEPT` between the saved snapshot (`FOR VERSION AS OF`) and the current one returns zero rows. Proves goal (a).

Decisions:

- "Bronze unchanged" means unchanged content, not "no new snapshot": a `MERGE` that rewrites identical values may still commit a snapshot (assumption, unverified).
- `EXCEPT` is set-based and misses duplicates, so the row-count and unique-PK checks stay.
- The snapshot comparison is a script with two explicit commands (`capture`, `compare`), not a rolling baseline: a rolling baseline would raise false failures whenever the source legitimately changed between runs, and continuous monitoring is Phase 3.
- The contract (content unchanged on rerun) is tool-independent; the mechanism (Iceberg time travel) is Iceberg-specific and accepted as such.
- Step 6 stays lean: Bronze ingestion may be rebuilt on CDC in Phase 2.
- `RUN_MODE` becomes a required command-line argument (`--mode init|incremental`, stdlib `argparse`); no default, so a bare run exits with an error. Reason: `INIT` drops every source table (`DROP TABLE ... CASCADE`) and still prints `SUCCESS!`, so a forgotten hand edit would silently wipe the source mid-procedure. The mode also becomes visible in the command written in the spec.

- Reconciliation after the `INCREMENTAL` seed + run #2 still demands an exact match with the source. This holds only because the seed issues no `DELETE` (fact: `scripts/initial_data.py` only inserts and updates). The Bronze `MERGE` has no delete branch and its source query never sees a deleted row, so any source delete would leave `brz_rows > src_rows` by the number of deleted rows: expected under the validity boundary, not a bug.

Still open: none. Written into the spec on 2026-11-24 (steps 6a–6g, Idempotency check, Validity boundary). Implementation split into tickets 05 (seed mode argument) and 06 (snapshot comparison script).
