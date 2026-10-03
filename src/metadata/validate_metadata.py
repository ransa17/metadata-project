#!/usr/bin/env python3
"""Metadata quality gate — fails the job if documentation is incomplete.

Runs as a bundle task after all apply_metadata_* tasks. It answers one
question: "can a new table or column ship undocumented?" — and the answer
must be no. Checks per layer:

  1. Every metadata YAML parses and satisfies the schema (required fields,
     enums — same rules apply_metadata.py enforces).
  2. Every live Unity Catalog table in the layer schema has a metadata
     YAML file (an undocumented new table fails the gate).
  3. Every documented table exists live.
  4. The live column set matches the documented column set (schema drift,
     e.g. a new undocumented column, fails the gate).
  5. Live UC state: non-empty table comment, non-empty column comments,
     required table tags (layer, source_system, owner, refresh_frequency)
     and required column tags (pii, semantic_type, ai_role).

Prints a coverage percentage and exits 1 if coverage is below the threshold
(default 100) or any structural issue was found.

Usage:

    python validate_metadata.py \
        --metadata-root /Workspace/.../metadata \
        --layer-schemas bronze=...,silver=...,gold=... \
        --min-coverage 100
"""

import argparse
import sys

import yaml

LAYERS = ("bronze", "silver", "gold")
REQUIRED_TABLE_FIELDS = (
    "name", "layer", "description", "grain", "owner",
    "source_system", "refresh_frequency",
)
REQUIRED_TABLE_TAGS = ("layer", "source_system", "owner", "refresh_frequency")
REQUIRED_COLUMN_FIELDS = ("description", "pii", "semantic_type", "ai_role")
COLUMN_TAG_FIELDS = ("pii", "semantic_type", "ai_role")
VALID_SEMANTIC_TYPES = {"identifier", "measure", "dimension", "timestamp"}
VALID_AI_ROLES = {"feature", "label", "key", "exclude"}


def esc(value):
    return str(value).replace("'", "''")


def parse_layer_schemas(raw):
    mapping = {}
    for part in raw.split(","):
        layer, sep, location = part.strip().partition("=")
        location = location.strip()
        if not sep or layer not in LAYERS or location.count(".") != 1:
            raise ValueError(
                f"Invalid layer spec {part!r} — expected '<layer>=<catalog>.<schema>'"
            )
        mapping[layer] = location
    return mapping


def validate_spec(path, spec):
    """Same YAML validation rules as apply_metadata.py."""
    errors = []
    table = spec.get("table") or {}
    name = table.get("name")
    if not name:
        return [f"{path}: table.name is missing"]
    for field in REQUIRED_TABLE_FIELDS:
        if not str(table.get(field) or "").strip():
            errors.append(f"{path}: table.{field} is missing or empty")
    tags = table.get("tags") or {}
    for tag in REQUIRED_TABLE_TAGS:
        if not str(tags.get(tag) or "").strip():
            errors.append(f"{path}: required table tag '{tag}' is missing or empty")
    columns = spec.get("columns") or {}
    if not columns:
        errors.append(f"{path}: no columns documented")
    for col, colspec in columns.items():
        for field in REQUIRED_COLUMN_FIELDS:
            value = colspec.get(field)
            if field == "pii":
                if not isinstance(value, bool):
                    errors.append(f"{path}: column {col}.pii must be true or false")
            elif not str(value or "").strip():
                errors.append(f"{path}: column {col}.{field} is missing or empty")
        if colspec.get("semantic_type") not in VALID_SEMANTIC_TYPES:
            errors.append(
                f"{path}: column {col}.semantic_type must be one of "
                f"{sorted(VALID_SEMANTIC_TYPES)}"
            )
        if colspec.get("ai_role") not in VALID_AI_ROLES:
            errors.append(
                f"{path}: column {col}.ai_role must be one of {sorted(VALID_AI_ROLES)}"
            )
    return errors


def fetch(spark, query):
    return [row.asDict() for row in spark.sql(query).collect()]


def main():
    parser = argparse.ArgumentParser(description="Metadata quality gate.")
    parser.add_argument("--metadata-root", required=True)
    parser.add_argument(
        "--layer-schemas", required=True,
        help="bronze=<catalog>.<schema>,silver=...,gold=...",
    )
    parser.add_argument(
        "--min-coverage", type=float, default=100.0,
        help="Minimum coverage percentage required to pass (default 100)",
    )
    args = parser.parse_args()

    try:
        layer_map = parse_layer_schemas(args.layer_schemas)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        return 1

    import os

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    issues = []          # structural problems (undocumented table, drift, bad YAML)
    total_checks = 0     # every individual documented fact we expect to hold
    failed_checks = 0

    print("=" * 72)
    print("METADATA QUALITY GATE")
    print("=" * 72)

    for layer in LAYERS:
        if layer not in layer_map:
            continue
        catalog, schema = layer_map[layer].split(".")

        # ---- Load documented metadata ------------------------------------
        import os as _os
        layer_dir = _os.path.join(args.metadata_root, layer)
        specs = {}
        if _os.path.isdir(layer_dir):
            for fname in sorted(_os.listdir(layer_dir)):
                if not fname.endswith(".yml"):
                    continue
                path = _os.path.join(layer_dir, fname)
                with open(path, "r", encoding="utf-8") as handle:
                    spec = yaml.safe_load(handle)
                total_checks += 1  # "this YAML is valid" is itself a check
                yaml_errors = validate_spec(path, spec or {})
                if yaml_errors:
                    failed_checks += 1
                    issues.extend(yaml_errors)
                if isinstance(spec, dict) and spec.get("table", {}).get("name"):
                    specs[spec["table"]["name"]] = spec

        # ---- Live Unity Catalog state ------------------------------------
        ischema = f"{catalog}.information_schema"
        live_rows = fetch(
            spark,
            f"SELECT table_name, comment, table_type FROM {ischema}.tables "
            f"WHERE table_schema = '{esc(schema)}'",
        )
        # Ignore SDP-internal backing objects (__materialization_* backing
        # tables and event_log_* tables) — they are not user-facing datasets.
        def _internal(name):
            return name.startswith("__materialization_") or name.startswith("event_log_")

        live_rows = [row for row in live_rows if not _internal(row["table_name"])]
        live_tables = {
            row["table_name"]: (row.get("comment") or "") for row in live_rows
        }
        table_types = {
            row["table_name"]: (row.get("table_type") or "UNKNOWN") for row in live_rows
        }
        table_tags = {
            (row["table_name"], row["tag_name"])
            for row in fetch(
                spark,
                f"SELECT table_name, tag_name FROM {ischema}.table_tags "
                f"WHERE catalog_name = '{esc(catalog)}' "
                f"AND schema_name = '{esc(schema)}'",
            )
        }
        live_columns = {}
        for row in fetch(
            spark,
            f"SELECT table_name, column_name, comment FROM {ischema}.columns "
            f"WHERE table_schema = '{esc(schema)}'",
        ):
            live_columns.setdefault(row["table_name"], {})[row["column_name"]] = (
                row.get("comment") or ""
            )
        column_tags = {
            (row["table_name"], row["column_name"], row["tag_name"])
            for row in fetch(
                spark,
                f"SELECT table_name, column_name, tag_name FROM {ischema}.column_tags "
                f"WHERE catalog_name = '{esc(catalog)}' "
                f"AND schema_name = '{esc(schema)}'",
            )
        }

        # ---- Structural checks -------------------------------------------
        undocumented = sorted(set(live_tables) - set(specs))
        for table_name in undocumented:
            total_checks += 1
            failed_checks += 1
            issues.append(
                f"{layer}: table '{table_name}' exists in UC but has NO metadata YAML "
                f"(add metadata/{layer}/{table_name}.yml)"
            )
        for table_name in sorted(set(specs) - set(live_tables)):
            total_checks += 1
            failed_checks += 1
            issues.append(
                f"{layer}: table '{table_name}' is documented but does not exist in "
                f"{catalog}.{schema}"
            )

        # ---- Coverage checks on documented + live tables ------------------
        layer_total = 0
        layer_failed = 0
        for table_name, spec in sorted(specs.items()):
            if table_name not in live_tables:
                continue
            documented_columns = set(spec.get("columns") or {})
            actual_columns = set(live_columns.get(table_name, {}))
            if documented_columns != actual_columns:
                total_checks += 1
                layer_total += 1
                failed_checks += 1
                layer_failed += 1
                missing_docs = sorted(actual_columns - documented_columns)
                extra_docs = sorted(documented_columns - actual_columns)
                detail = []
                if missing_docs:
                    detail.append(f"undocumented column(s): {missing_docs}")
                if extra_docs:
                    detail.append(f"documented but absent: {extra_docs}")
                issues.append(
                    f"{layer}.{table_name}: column drift — {'; '.join(detail)}"
                )

            # Table comment
            total_checks += 1
            layer_total += 1
            if not live_tables[table_name].strip():
                failed_checks += 1
                layer_failed += 1
                issues.append(f"{layer}.{table_name}: table comment missing in UC")

            # Required table tags
            for tag in REQUIRED_TABLE_TAGS:
                total_checks += 1
                layer_total += 1
                if (table_name, tag) not in table_tags:
                    failed_checks += 1
                    layer_failed += 1
                    issues.append(
                        f"{layer}.{table_name}: required table tag '{tag}' missing in UC"
                    )

            # Column comments (real tables and materialized views; plain
            # views have no DDL for column metadata) + required column tags
            # (all object types)
            is_plain_view = table_types.get(table_name) == "VIEW"
            for column in sorted(actual_columns):
                if not is_plain_view:
                    total_checks += 1
                    layer_total += 1
                    if not live_columns[table_name][column].strip():
                        failed_checks += 1
                        layer_failed += 1
                        issues.append(
                            f"{layer}.{table_name}.{column}: column comment missing in UC"
                        )
                for tag in COLUMN_TAG_FIELDS:
                    total_checks += 1
                    layer_total += 1
                    if (table_name, column, tag) not in column_tags:
                        failed_checks += 1
                        layer_failed += 1
                        issues.append(
                            f"{layer}.{table_name}.{column}: column tag '{tag}' "
                            "missing in UC"
                        )

        if layer_total:
            coverage = 100.0 * (layer_total - layer_failed) / layer_total
            print(
                f"{layer:>7} | {catalog}.{schema} | "
                f"{len(live_tables)} live table(s) | coverage {coverage:.1f}%"
            )

    overall = (
        100.0 * (total_checks - failed_checks) / total_checks
        if total_checks
        else 0.0
    )
    print("-" * 72)
    print(f"OVERALL METADATA COVERAGE: {overall:.1f}% (threshold {args.min_coverage:.0f}%)")

    if issues:
        print("\nIssues found:")
        for issue in issues:
            print(f"  - {issue}")

    if issues or overall < args.min_coverage:
        print(
            "\nFAIL: metadata gate did not pass — every table and column must have "
            "a description and required tags before this job can succeed."
        )
        return 1

    print("\nPASS: all tables and columns are documented, tagged, and applied.")
    return 0


if __name__ == "__main__":
    # Exit non-zero ONLY on failure: the job runner treats any SystemExit
    # (even exit code 0) as a task failure, so return cleanly on success.
    _exit_code = main()
    if _exit_code != 0:
        raise SystemExit(_exit_code)
