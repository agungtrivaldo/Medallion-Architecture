# 02: Redesign M0.1 step 6 (MERGE + idempotency test)

**What to build:** A decision, proposed by the owner, on how step 6 proves two separate things: (a) idempotency, meaning a DAG rerun with no source changes leaves Bronze unchanged, and (b) `MERGE` correctness, meaning new and updated source rows land in Bronze. The original step 6 would pass vacuously: the `INIT` seed's `updated_at` values all fall before 2026-03-28, so a `MERGE` filtered on `data_interval_start` receives zero rows. The decision is written into the spec's contract as the new step 6.

**Blocked by:** None (can start immediately). Recommended after 01, so the owner has seen a real DAG run first.

**Status:** needs-triage

- [ ] The owner proposes the order of DAG runs and `INCREMENTAL` seeds, and it is evaluated against both goals (a) and (b).
- [ ] The design states what each DAG run must show, including which tables are expected to take the `MERGE` path and which take CTAS.
- [ ] The design checks that the `INCREMENTAL` seed's changes actually reach the `MERGE` filter (rows with `updated_at` inside the run's data interval), so no check passes with zero rows.
- [ ] Decided: whether `RUN_MODE` stays a hand edit in the seed script or becomes a command-line argument.
- [ ] Decided: how reconciliation is interpreted after an `INCREMENTAL` seed (the validity boundary still excludes deletes and multiple changes per row between runs).
- [ ] The spec's step 6 and validity boundary are updated with the agreed design.
