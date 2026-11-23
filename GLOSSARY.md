# Medallion Lakehouse

A local data platform that moves operational data through Bronze, Silver, and Gold layers so that every downstream dataset can be traced back to, and reconciled against, its source.

## Language

### Layers

**Bronze**:
The append-only record of every change captured from a source, stored with its original content plus ingestion metadata; nothing in it is ever updated or overwritten, and source deletes are recorded as change events rather than inferred from a row's absence.
_Avoid_: Mirror, replica, raw copy, staging

### Correctness

**Reconciliation**:
A comparison of a layer's contents against its source (row counts, key uniqueness, and totals of key amounts) that proves data arrived completely and unaltered, independent of the mechanism that moved it.
_Avoid_: Monitoring, data validation, pipeline check
