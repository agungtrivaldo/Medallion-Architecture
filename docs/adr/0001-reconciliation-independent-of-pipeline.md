---
status: accepted
date: 2026-11-23
---

# Reconciliation is configured independently of the pipeline it checks

The Bronze reconciliation (`scripts/reconcile_bronze.py`) reads its table list, primary keys, and money columns from its own config, `scripts/reconcile_bronze.yaml`, written from the source schema (`information_schema` in `postgres_oltp`). It does not read the DAG's `TABLE` dict or any other pipeline config. A checker that shares the pipeline's assumptions shares its blind spots: if a table is dropped from the DAG's dict, a checker reading that dict stops checking it and still reports "All tables PASSED".

## Considered Options

- **Read the table list and keys from the DAG's `TABLE` dict.** One list to maintain, but any mistake in the dict becomes a mistake in the check. A table missing from the dict is never checked (a silent pass), and the DAG's wrong merge key for `user_addresses` (`user_id` instead of `address_id`) would have become the checker's key too.
- **Trust Airflow task status.** A task can succeed while rows are missing, duplicated, or altered; task status says the pipeline ran, not that the data is right.
- **Checks inside the pipeline** (e.g. tests as a DAG task). Useful later, but they run on the pipeline's own config and so inherit the same blind spots.

## Consequences

- Two lists describe the same tables: the DAG's dict and the reconciliation YAML. Adding a source table means updating both. This duplication is deliberate; do not "fix" it by deriving one from the other.
- A table that exists in the source but is missing from the YAML is not checked. Keep the YAML in step with the source schema, not with the DAG.
- The same rule applies when reconciliation is extended to Silver, Gold, or a CDC pipeline: the checker's config comes from the source of truth, not from the pipeline under test.
