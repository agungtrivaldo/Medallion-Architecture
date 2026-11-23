"""Reconcile Bronze against the operational source.

For every table in the config file, compares the source (Trino catalog
`postgres`) with Bronze (Trino catalog `iceberg`, schema `bronze`):

  1. source count(*)          == Bronze count(*)
  2. Bronze count(DISTINCT pk) == Bronze count(*)      (no duplicate keys)
  3. source sum(money column) ~= Bronze sum(money column), per money column

Rules are defined in `.scratch/phase-0/spec.md` ("Reconciliation checks").
A source table with zero rows fails, so the check cannot pass vacuously.

Exits 0 when every table passes, 1 when any table fails, and 2 when the
config file is invalid, so it can be used as a regression check after
pipeline changes.

Usage:
    .venv/bin/python scripts/reconcile_bronze.py
    .venv/bin/python scripts/reconcile_bronze.py --config path/to/tables.yaml

Connection defaults to Trino on localhost:8080; override with TRINO_HOST,
TRINO_PORT, TRINO_USER.
"""

import argparse
import os
import re
import sys
from decimal import Decimal
from pathlib import Path

import trino
import yaml

DEFAULT_CONFIG = Path(__file__).with_suffix(".yaml")

SOURCE = "postgres.public"
BRONZE = "iceberg.bronze"

# Bronze casts money columns from NUMERIC to DOUBLE, which can lose precision.
# Amounts are in rupiah, so a difference below 1 rupiah is rounding noise; a
# missing or wrong row moves the sum by at least the smallest price (thousands).
SUM_TOLERANCE = Decimal("1")

# Names from the config are put into SQL text, so only plain identifiers are
# accepted.
IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")
TABLE_KEYS = {"pk", "amount_columns"}


class ConfigError(Exception):
    pass


def load_config(path: Path) -> dict:
    """Read and validate the config. Any mistake stops the run before a query.

    Strict on purpose: a silently ignored typo (e.g. `amount_column`) would
    skip a check and let the table pass without being checked.
    """
    try:
        raw = yaml.safe_load(path.read_text())
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigError(f"cannot read {path}: {exc}") from exc

    if not isinstance(raw, dict) or set(raw) != {"tables"}:
        raise ConfigError("top level must contain exactly one key: tables")
    tables = raw["tables"]
    if not isinstance(tables, dict) or not tables:
        raise ConfigError("tables must be a non-empty mapping")

    for table, config in tables.items():
        where = f"tables.{table}"
        if not isinstance(table, str) or not IDENTIFIER.match(table):
            raise ConfigError(f"{where}: invalid table name")
        if not isinstance(config, dict) or set(config) != TABLE_KEYS:
            raise ConfigError(f"{where}: must have exactly the keys {sorted(TABLE_KEYS)}")
        if not isinstance(config["pk"], str) or not IDENTIFIER.match(config["pk"]):
            raise ConfigError(f"{where}.pk: must be a column name")
        columns = config["amount_columns"]
        if not isinstance(columns, list) or not all(
            isinstance(c, str) and IDENTIFIER.match(c) for c in columns
        ):
            raise ConfigError(f"{where}.amount_columns: must be a list of column names")
        if len(set(columns)) != len(columns):
            raise ConfigError(f"{where}.amount_columns: duplicate column")
    return tables


def build_query(table: str, pk: str, amount_columns: list[str]) -> str:
    """One query per table that returns every number the checks need."""
    src, brz = f"{SOURCE}.{table}", f"{BRONZE}.{table}"
    selects = [
        f"(SELECT count(*) FROM {src}) AS src_rows",
        f"(SELECT count(*) FROM {brz}) AS brz_rows",
        f"(SELECT count(DISTINCT {pk}) FROM {brz}) AS brz_unique_pk",
    ]
    for col in amount_columns:
        # Trino reads unbounded Postgres NUMERIC as type `number`; casting the
        # sums to DECIMAL gives exact values on the source side and a Python
        # Decimal on both sides.
        selects.append(f"(SELECT CAST(sum({col}) AS DECIMAL(38, 4)) FROM {src}) AS src_sum_{col}")
        selects.append(f"(SELECT CAST(sum({col}) AS DECIMAL(38, 4)) FROM {brz}) AS brz_sum_{col}")
    return "SELECT\n  " + ",\n  ".join(selects)


def sums_match(src: Decimal | None, brz: Decimal | None) -> bool:
    """sum() is NULL when every value is NULL; treat NULL == NULL as a match."""
    if src is None or brz is None:
        return src is None and brz is None
    return abs(src - brz) <= SUM_TOLERANCE


def check_table(cursor, table: str, config: dict) -> tuple[bool, dict, list[str]]:
    """Run the checks for one table. Returns (passed, numbers, failure reasons)."""
    amount_columns = config["amount_columns"]
    cursor.execute(build_query(table, config["pk"], amount_columns))
    row = cursor.fetchone()
    names = [d[0] for d in cursor.description]
    numbers = dict(zip(names, row))

    failures = []
    if numbers["src_rows"] == 0:
        failures.append("source is empty (check would pass vacuously)")
    if numbers["src_rows"] != numbers["brz_rows"]:
        failures.append(f"row count: source {numbers['src_rows']} != bronze {numbers['brz_rows']}")
    if numbers["brz_unique_pk"] != numbers["brz_rows"]:
        failures.append(
            f"duplicate {config['pk']} in bronze: "
            f"{numbers['brz_rows'] - numbers['brz_unique_pk']} extra rows"
        )
    for col in amount_columns:
        src_sum, brz_sum = numbers[f"src_sum_{col}"], numbers[f"brz_sum_{col}"]
        if not sums_match(src_sum, brz_sum):
            failures.append(f"sum({col}): source {src_sum} != bronze {brz_sum}")

    return not failures, numbers, failures


def format_amount(value: Decimal | None) -> str:
    return "NULL" if value is None else f"{value:,.2f}"


# One report table: counts, then the exact sums behind the money check, so the
# evidence is visible at a glance. A table with no money column shows "-"; a
# table with several gets one continuation line per extra column.
COLUMNS = [  # (header, width, align)
    ("table", 16, "<"),
    ("src_rows", 10, ">"),
    ("brz_rows", 10, ">"),
    ("unique_pk", 11, ">"),
    ("money_column", 26, "<"),
    ("src_sum", 20, ">"),
    ("brz_sum", 20, ">"),
    ("diff", 10, ">"),
    ("result", 8, ">"),
]


def format_row(values: list) -> str:
    cells = []
    for i, ((_, width, align), value) in enumerate(zip(COLUMNS, values)):
        text = str(value)
        if align == "<" and i > 0:
            text = "  " + text  # gap before a left-aligned column
        cells.append(f"{text:{align}{width}}")
    return "".join(cells).rstrip()


def report_lines(table: str, amount_columns: list[str], numbers: dict | None, passed: bool) -> list[str]:
    result = "PASS" if passed else "FAIL"
    if numbers is None:  # the query itself failed
        return [format_row([table, "?", "?", "?", "?", "?", "?", "?", result])]

    counts = [numbers["src_rows"], numbers["brz_rows"], numbers["brz_unique_pk"]]
    if not amount_columns:
        return [format_row([table, *counts, "-", "-", "-", "-", result])]

    lines = []
    for i, col in enumerate(amount_columns):
        src, brz = numbers[f"src_sum_{col}"], numbers[f"brz_sum_{col}"]
        diff = None if src is None or brz is None else brz - src
        sums = [col, format_amount(src), format_amount(brz), format_amount(diff)]
        if i == 0:
            lines.append(format_row([table, *counts, *sums, result]))
        else:
            lines.append(format_row(["", "", "", "", *sums, ""]))
    return lines


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconcile Bronze against the operational source.")
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help=f"YAML file listing the tables to check (default: {DEFAULT_CONFIG.name} next to this script)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        tables = load_config(args.config)
    except ConfigError as exc:
        print(f"Invalid config: {exc}", file=sys.stderr)
        return 2

    conn = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "localhost"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "reconciliation"),
    )
    cursor = conn.cursor()

    print(f"Config: {args.config}")
    header = format_row([name for name, _, _ in COLUMNS])
    print(header)
    print("-" * len(header))

    all_failures = {}
    for table, config in tables.items():
        try:
            passed, numbers, failures = check_table(cursor, table, config)
        except trino.exceptions.TrinoQueryError as exc:
            # A missing Bronze table or column fails this table, not the run.
            passed, numbers = False, None
            failures = [f"query failed: {exc.error_name}: {exc.message}"]

        if not passed:
            all_failures[table] = failures
        for line in report_lines(table, config["amount_columns"], numbers, passed):
            print(line)

    print()
    print(f"Sums: diff = brz_sum - src_sum; a sum passes when |diff| <= {SUM_TOLERANCE} rupiah.")
    print()
    if all_failures:
        print(f"{len(all_failures)} of {len(tables)} tables FAILED:")
        for table, failures in all_failures.items():
            for reason in failures:
                print(f"  {table}: {reason}")
        return 1

    print(f"All {len(tables)} tables PASSED.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
