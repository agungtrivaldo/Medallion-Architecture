# 03: Fix the `user_addresses` merge key

**What to build:** The Bronze `MERGE` for `user_addresses` matches rows on `address_id`, the table's real primary key, instead of `user_id`. Today the DAG uses `user_id`; the seeded data has 5,076 users with more than one address, so as soon as such a user's rows reach the `MERGE`, it sees several source rows for one target row.

**Blocked by:** 01 (the reconciliation script is the proof)

**Status:** needs-triage

- [ ] Owner approves the change to the DAG before it is made.
- [ ] Before the fix, the failure is reproduced: an `INCREMENTAL` seed followed by a DAG run makes the `user_addresses` upsert task fail (or the failure is shown not to happen, with the reason recorded here).
- [ ] The merge key for `user_addresses` is `address_id`.
- [ ] After an `INCREMENTAL` seed that touches at least one user with more than one address, the `user_addresses` upsert task succeeds.
- [ ] Reconciliation passes for `user_addresses`, including Bronze `count(DISTINCT address_id)` = Bronze `count(*)`.
- [ ] No other table's merge key changed.
