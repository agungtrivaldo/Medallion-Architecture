# 05: Seed mode as a required command-line argument

**What to build:** `scripts/initial_data.py` takes its mode from a required `--mode init|incremental` argument (stdlib `argparse`) instead of the hand-edited `RUN_MODE` constant. A run without `--mode` exits with an error and touches no table. Design decided in ticket 02.

**Blocked by:** 02

**Status:** needs-triage

- [ ] `python scripts/initial_data.py` with no argument exits non-zero with a usage message, and the source tables are unchanged.
- [ ] An unknown mode (e.g. `--mode foo`) exits non-zero and changes nothing.
- [ ] `--mode init` behaves as `RUN_MODE = "INIT"` did: 9 tables recreated, same row counts as the 2026-11-22 seed findings for the fixed-size tables (`brands` 8, `categories` 5, `products` 8, `users` 10,000).
- [ ] `--mode incremental` behaves as `RUN_MODE = "INCREMENTAL"` did: no table dropped, 50 new users, up to 200 new `ORD-INCR-` orders, up to 100 `pending` orders updated.
- [ ] The `RUN_MODE` constant is gone; the sizes and date range that depend on the mode are derived from the argument.
- [ ] No new dependency.

## Comments

2026-11-24: implemented. `--mode` is required (`argparse`, `choices=["init", "incremental"]`); argument parsing runs before any database connection. Verified: no argument and `--mode foo` both exit 2 with a usage message. The internal `RUN_MODE` variable remains, now derived from `--mode` instead of hand-edited. `--mode incremental` is exercised by step 6a; `--mode init` by ticket 04.
