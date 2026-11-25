# 04: M0.1 full acceptance run

**What to build:** One uninterrupted run of M0.1 steps 0–6g from a clean state (`docker compose down -v`), following the spec's contract exactly, with every result recorded. Passing this ticket means M0.1 is done.

**Blocked by:** 01, 02, 03, 05

**Status:** needs-triage (all checks passed 2026-11-25; awaiting owner review)

- [x] Starts from `docker compose down -v` with no manual edits to tracked files at any step.
- [x] Steps 0–3 pass as written in the contract (stack up, `bootstrap` exits 0 and `bronze` exists, 9 source tables seeded).
- [x] Steps 4–5 pass: DAG run #1 succeeds and reconciliation passes for all 9 tables.
- [x] Steps 6a–6g pass as written in the contract, including the validity guard (6c), so no check passes on zero rows.
- [x] Peak memory across the whole run is recorded.
- [x] `PROJECT_STATUS.md` shows every step passed with the date, and the milestone is marked done.
