# Source data model

Reference for the operational source database (`postgres_oltp`, database `oltp`, schema `public`) that the Bronze pipeline ingests. Everything here comes from `scripts/initial_data.py` and from queries against the seeded database.

Last checked: 2026-11-23, after an `INIT` seed.

## Domain

A simulated online electronics store selling hardware and gadgets (graphics cards, motherboards, smartphones, monitors, peripherals). Data is generated with Faker using the Indonesian locale (`id_ID`), and it is **dirty on purpose**: the seed script injects inconsistent casing, stray spaces, mixed phone formats, NULLs, and duplicate order lines (see [Intentional dirt](#intentional-dirt)).

## Relationships

```
brands ──────┐
             ├──▶ products ─────────────────┐
categories ──┘                              │
                                            ▼
users ──▶ user_addresses ──▶ orders ──▶ order_items
  │                            ▲  │
  └────────────────────────────┘  ├──▶ payments
                                  └──▶ shipping
```

An arrow means "is referenced by" (a foreign key on the right side points to the left side). For example, `orders.user_id` references `users`, and `order_items.order_id` references `orders`.

## Tables

**Grain** = what one row represents.

| Table | Grain (one row is) | PK | Foreign keys | Money columns (`numeric`) | Has `updated_at` | Rows (INIT) |
|---|---|---|---|---|---|---|
| `brands` | One brand (Asus, MSI, Apple, ...) | `brand_id` | — | — | No | 8 (fixed) |
| `categories` | One product category | `category_id` | — | — | No | 5 (fixed) |
| `products` | One product for sale | `product_id` | `category_id`, `brand_id` | `base_price` | Yes | 8 (fixed) |
| `users` | One customer | `user_id` | — | — | Yes | 10,000 (fixed) |
| `user_addresses` | One customer address | `address_id` | `user_id` | — | No | 15,076 (random) |
| `orders` | One order | `order_id` (text, e.g. `ORD-INIT-0-ABC123`) | `user_id`, `address_id` | `total_amount` | Yes | 50,000 (fixed) |
| `order_items` | One line in an order | `item_id` | `order_id`, `product_id` | `unit_price_at_purchase` | No | 104,854 (random) |
| `payments` | One payment for an order | `payment_id` | `order_id` | — | No | 41,726 (random) |
| `shipping` | One shipment for an order | `shipping_id` | `order_id` | `shipping_cost` | No | 41,726 (random) |

"Fixed" counts are set by constants in `scripts/initial_data.py`; "random" counts change with every seed, so compare against the source, never against the numbers above.

Master data (`brands`, `categories`, `products`) changes rarely. Customer data (`users`, `user_addresses`) and transactions (`orders` and its children) grow over time.

## Business flow

1. A **user** signs up and has 1–2 **addresses**. In the 2026-11-23 seed, 5,076 of the 10,000 users have two.
2. The user places an **order** with 1–3 **items**; each item is one product with quantity 1–2. The order picks one of the user's addresses (or none, 2% of the time).
3. `total_amount` = sum of (`unit_price_at_purchase` × `quantity`) + `shipping_cost`.
4. Order status is drawn from `Completed` (4/6), `Pending` (1/6), `Cancelled` (1/6).
5. Every order that is **not `Cancelled`** gets exactly one **payment** (`payment_status` always `Success`) and one **shipping** row. That is why `payments` and `shipping` have the same count, lower than `orders`.
6. Shipping status is `Delivered` for `Completed` orders (with courier and tracking number) and `Packed` for `Pending` orders (no tracking number; courier missing 20% of the time).

### `INCREMENTAL` mode

With `RUN_MODE = "INCREMENTAL"`, the script adds 50 users and 200 orders dated within the last day, then sets up to 100 `Pending` orders to `Completed` with `updated_at = NOW()`. Order IDs use the prefix `ORD-INCR-`.

## Intentional dirt

| Dirt | Example | Where |
|---|---|---|
| Random casing and padding | `"  JAWA BARAT "`, `"cOMPLETED"` | Most text columns, including category names, statuses, payment methods, couriers, emails, provinces, cities |
| Mixed phone formats | `+62...`, `62...`, `0...`, padded with spaces | `users.phone_number` |
| NULLs | 15% `phone_number`, 10% `postal_code`, 2% `orders.address_id`, 1% `unit_price_at_purchase` | Several tables |
| NULL order total | `total_amount` is NULL when any item has no price, plus 1% at random | `orders` |
| Duplicate order lines | 5% of items are inserted twice | `order_items` |

The duplicates in `order_items` are **business duplicates, not PK duplicates**: each copy gets a new `item_id` from `SERIAL`, so the primary key stays unique.

## What this means for the pipeline

- **Bronze keeps the dirt.** Bronze stores data as received, so all of the above must appear in Bronze too. Cleaning (trimming, casing, phone normalization, de-duplicating order lines) is Silver's job.
- **Reconciliation checks sameness, not cleanliness.** A dirty value that matches the source is a pass.
- **A PK uniqueness check will not catch the duplicate order lines**, because their `item_id`s differ.
- **Only `users`, `products`, and `orders` have `updated_at`.** The child tables do not, which is why the DAG joins them to a parent to borrow a watermark (a known deviation, see `.scratch/phase-0/spec.md`).
- **The DAG's merge key for `user_addresses` is wrong.** The source PK is `address_id`; the DAG uses `user_id` (ticket `.scratch/phase-0/issues/03-fix-user-addresses-merge-key.md`).
- **Money columns are cast to `DOUBLE` in Bronze**, so source and Bronze sums are compared with a small tolerance.
