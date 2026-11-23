# 04: M0.1 full acceptance run

**What to build:** One uninterrupted run of M0.1 steps 0–6 from a clean state (`docker compose down -v`), following the spec's contract exactly, with every result recorded. Passing this ticket means M0.1 is done.

**Blocked by:** 01, 02, 03

**Status:** needs-triage

- [ ] Starts from `docker compose down -v` with no manual edits to tracked files at any step.
- [ ] Steps 0–3 pass as written in the contract (stack up, `bootstrap` exits 0 and `bronze` exists, 9 source tables seeded).
- [ ] Steps 4–5 pass: DAG run #1 succeeds and reconciliation passes for all 9 tables.
- [ ] Step 6 passes as redesigned in 02, with no check passing on zero rows.
- [ ] Peak memory across the whole run is recorded.
- [ ] `PROJECT_STATUS.md` shows every step passed with the date, and the milestone is marked done.
