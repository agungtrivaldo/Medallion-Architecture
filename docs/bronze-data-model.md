# Bronze data model

Reference for the Bronze layer: where the tables live, how they are stored and written, and what each column is. Everything here comes from `dags/etl_pipeline.py` and from `SHOW CREATE TABLE` / queries against the running stack. For the source tables Bronze copies, see [source-data-model.md](source-data-model.md).

Last checked: 2026-11-24, after DAG run #5 of the step 6 development run (see `.scratch/phase-0/spec.md`, findings log).

## Storage

| Property | Value |
|---|---|
| Query engine | Trino, catalog `iceberg` |
| Table format | Apache Iceberg, `format_version = 2` |
| Data file format | Parquet |
| Catalog / metastore | Hive Metastore (stores each table's name and current metadata pointer) |
| Object storage | MinIO, bucket `warehouse` |
| Schema | `iceberg.bronze`, location `s3a://warehouse/` |
| Table location | `s3a://warehouse/<table>-<uuid>/` (the UUID is assigned at creation) |
| Partitioning | None (no `partitioning` property on any table) |
| History | Every write commits a new Iceberg snapshot; older snapshots stay readable with `FOR VERSION AS OF <snapshot_id>`, including across `CREATE OR REPLACE TABLE` |

All 9 tables share the same Trino types: integers stay `integer`, text becomes `varchar`, source `timestamp` becomes `timestamp(6)`, and source `numeric` money columns become `double`.

## How the tables are written

DAG `etl_pipeline` (`@daily`, `catchup=False`). For each table, `table_checking` picks one path:

- **CTAS** (`create_table_<table>`): `CREATE OR REPLACE TABLE ... AS <query>`, a full rewrite. Taken when the table does not exist, is empty, or has no watermark.
- **MERGE** (`upsert_table_<table>`): `MERGE INTO` with the rows whose watermark is `>= data_interval_start`. Matched rows are updated, new rows inserted, both with every Bronze column (the column list comes from the Bronze table). No delete branch.

| Table | Path after the first load | Watermark | Merge key | Columns borrowed by a join |
|---|---|---|---|---|
| `brands` | CTAS every run | none | `brand_id` | — |
| `categories` | CTAS every run | none | `category_id` | — |
| `products` | CTAS every run | none | `product_id` | — |
| `users` | MERGE | `updated_at` | `user_id` | — |
| `user_addresses` | MERGE | `updated_at` (from `users`) | `address_id` | `created_at`, `updated_at` from `users` |
| `orders` | MERGE | `updated_at` | `order_id` | — |
| `order_items` | MERGE | `updated_at` (from `orders`) | `item_id` | `updated_at` from `orders` |
| `payments` | MERGE | `updated_at` (from `orders`) | `payment_id` | `updated_at` from `orders` |
| `shipping` | MERGE | `updated_at` (from `orders`) | `shipping_id` | `updated_at` from `orders` |

The joins exist only to give child tables a watermark; they are a known deviation from the target Bronze definition (spec, Known deviations).

## Metadata columns

Every Bronze table ends with two columns that the source does not have:

| Column | Type | Value |
|---|---|---|
| `_ingested_at` | `timestamp(6) with time zone` | `current_timestamp` when the DAG query ran |
| `_source_system` | `varchar` | Always `'postgres_oltp'` |

`_ingested_at` is re-stamped whenever a row is written: on CTAS tables every run (the whole table is rewritten), on `MERGE` tables for every row inside the filter window. It is therefore excluded from the idempotency content check; `updated_at`, by contrast, is source data and must not change on a rerun.

## Columns per table

Origin: **source** = copied unchanged; **cast** = copied with a type change; **join** = borrowed from a parent table; **meta** = added by the DAG.

### `brands`

| Column | Type | Origin |
|---|---|---|
| `brand_id` | integer | source (PK) |
| `brand_name` | varchar | source |
| `country_of_origin` | varchar | source |
| `_ingested_at`, `_source_system` | see Metadata columns | meta |

### `categories`

| Column | Type | Origin |
|---|---|---|
| `category_id` | integer | source (PK) |
| `category_name` | varchar | source |
| `_ingested_at`, `_source_system` | see Metadata columns | meta |

### `products`

| Column | Type | Origin |
|---|---|---|
| `product_id` | integer | source (PK) |
| `category_id`, `brand_id` | integer | source (FK) |
| `product_name` | varchar | source |
| `base_price` | double | cast from `numeric` |
| `weight_grams` | integer | source |
| `updated_at` | timestamp(6) | source |
| `_ingested_at`, `_source_system` | see Metadata columns | meta |

### `users`

| Column | Type | Origin |
|---|---|---|
| `user_id` | integer | source (PK) |
| `email`, `password_hash`, `phone_number` | varchar | source |
| `created_at`, `updated_at` | timestamp(6) | source |
| `_ingested_at`, `_source_system` | see Metadata columns | meta |

### `user_addresses`

| Column | Type | Origin |
|---|---|---|
| `address_id` | integer | source (PK) |
| `user_id` | integer | source (FK) |
| `province`, `city`, `postal_code`, `full_address` | varchar | source |
| `created_at`, `updated_at` | timestamp(6) | join, from `users` |
| `_ingested_at`, `_source_system` | see Metadata columns | meta |

### `orders`

Column order differs from the source: `user_id` comes first.

| Column | Type | Origin |
|---|---|---|
| `user_id` | integer | source (FK) |
| `order_id` | varchar | source (PK, e.g. `ORD-INIT-0-ABC123`) |
| `total_amount` | double | cast from `numeric` |
| `address_id` | integer | source (FK) |
| `order_date`, `updated_at` | timestamp(6) | source |
| `order_status` | varchar | source |
| `_ingested_at`, `_source_system` | see Metadata columns | meta |

### `order_items`

| Column | Type | Origin |
|---|---|---|
| `item_id` | integer | source (PK) |
| `order_id` | varchar | source (FK) |
| `product_id`, `quantity` | integer | source |
| `unit_price_at_purchase` | double | cast from `numeric` |
| `updated_at` | timestamp(6) | join, from `orders` |
| `_ingested_at`, `_source_system` | see Metadata columns | meta |

### `payments`

| Column | Type | Origin |
|---|---|---|
| `payment_id` | integer | source (PK) |
| `order_id` | varchar | source (FK) |
| `payment_method`, `payment_status` | varchar | source |
| `payment_date` | timestamp(6) | source |
| `updated_at` | timestamp(6) | join, from `orders` |
| `_ingested_at`, `_source_system` | see Metadata columns | meta |

### `shipping`

| Column | Type | Origin |
|---|---|---|
| `shipping_id` | integer | source (PK) |
| `order_id` | varchar | source (FK) |
| `courier_name`, `tracking_number`, `shipping_status` | varchar | source |
| `shipping_cost` | double | cast from `numeric` |
| `updated_at` | timestamp(6) | join, from `orders` |
| `_ingested_at`, `_source_system` | see Metadata columns | meta |

## Known issues

Accepted deviations (overwrite instead of append, deletes not captured, joins, lossy `double` casts, daily granularity) are listed in the spec's Known deviations.

Fixed 2026-11-24: the `MERGE` path used to build its column list from the source table, so Bronze-only columns (**join** and **meta** above) were `NULL` on inserted rows and stale on updated rows. Reconciliation (counts, unique PK, money sums) cannot see this kind of defect; the step 6 idempotency check and direct `NULL` checks found it.

## Inspecting a table

```sql
SHOW CREATE TABLE iceberg.bronze.orders;                     -- columns, types, format, location
SELECT * FROM iceberg.bronze."orders$snapshots";              -- snapshot history
SELECT * FROM iceberg.bronze.orders FOR VERSION AS OF <id>;   -- time travel
```

Run them with `docker exec trino trino --execute "<sql>"`.
