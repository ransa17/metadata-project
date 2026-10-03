#!/usr/bin/env python3
"""Apply metadata/<layer>/<table>.yml definitions to Unity Catalog.

Runs as a bundle task immediately after each medallion layer's pipeline
task. Idempotent: comments, tags and table properties are overwritten in
place on every run, and PRIMARY KEY / FOREIGN KEY constraints that already
exist are skipped.

Per table this applies:
  - table comment            COMMENT ON TABLE
  - table properties         ALTER TABLE ... SET TBLPROPERTIES
  - table tags               ALTER TABLE ... SET TAGS
  - column comments          ALTER TABLE ... ALTER COLUMN ... COMMENT
  - column tags              pii / semantic_type / ai_role
  - informational PK and FK   ALTER TABLE ... ADD CONSTRAINT

Usage (parameters are supplied by the orchestration job in
resources/orchestration_job.yml):

    python apply_metadata.py \
        --layer bronze \
        --catalog workspace \
        --schema medallion_bronze \
        --metadata-root /Workspace/.../metadata \
        --layer-schemas bronze=workspace.medallion_bronze,silver=...,gold=...

Exit codes: 0 = applied; 2 = invalid YAML; 3 = table not found in UC
(the apply task must run AFTER the layer's pipeline task has created it).
"""

import argparse
import os
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
    """Escape a value for use inside a single-quoted SQL string literal."""
    return str(value).replace("'", "''")


def tag_value(value):
    """Normalize a value (bool/str) for a UC tag."""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def parse_layer_schemas(raw):
    """Parse 'bronze=cat.schema,silver=cat.schema,...' into a dict."""
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


def sql_list(items):
    return "(" + ", ".join(str(i) for i in items) + ")"


def validate_spec(path, spec):
    """Validate one metadata YAML document; returns a list of error strings."""
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


def load_specs(metadata_root, layer):
    """Load and validate every YAML file for a layer.

    Returns (specs, errors) where specs is a list of (path, spec) tuples.
    """
    layer_dir = os.path.join(metadata_root, layer)
    if not os.path.isdir(layer_dir):
        return [], [f"Metadata directory not found: {layer_dir}"]
    specs, errors = [], []
    for fname in sorted(os.listdir(layer_dir)):
        if not fname.endswith(".yml"):
            continue
        path = os.path.join(layer_dir, fname)
        with open(path, "r", encoding="utf-8") as handle:
            spec = yaml.safe_load(handle)
        if not isinstance(spec, dict) or "table" not in spec:
            errors.append(f"{path}: missing top-level 'table' section")
            continue
        errors.extend(validate_spec(path, spec))
        specs.append((path, spec))
    return specs, errors


def add_constraint(spark, full_name, constraint_name, ddl):
    """Add an informational constraint; skip if it already exists."""
    try:
        spark.sql(f"ALTER TABLE {full_name} ADD CONSTRAINT {constraint_name} {ddl}")
        return "added"
    except Exception as exc:  # noqa: BLE001 - inspect message, never re-raise
        message = str(exc).lower()
        if "already exists" in message:
            return "already-present"
        print(f"    WARN constraint {constraint_name} not applied: {exc}")
        return "failed"


VIEW_TYPES = {"VIEW", "MATERIALIZED_VIEW"}


def apply_one_table(spark, spec, full_name, layer, layer_map, obj_type="UNKNOWN"):
    """Apply all metadata for one table. Returns a summary string.

    Object-type aware: dev-mode SDP pipelines publish MATERIALIZED_VIEW
    objects; production pipelines publish Delta tables. Pipeline views
    support comments, table tags, column comments and column tags
    (via ALTER MATERIALIZED VIEW / ALTER VIEW), but not table properties
    or PK/FK constraints — those apply to real tables only.
    """
    table = spec["table"]
    is_view = obj_type in VIEW_TYPES

    # 1. Table comment (idempotent — overwrites)
    spark.sql(f"COMMENT ON TABLE {full_name} IS '{esc(table['description'].strip())}'")

    # 2. Table properties — real tables only (views reject SET TBLPROPERTIES;
    #    their metadata travels via tags instead).
    notes = []
    properties = table.get("properties") or {}
    if properties:
        if is_view:
            # Pipeline-published views reject SET TBLPROPERTIES
            # (SET_TBLPROPERTIES_NOT_ALLOWED_FOR_PIPELINE_TABLE) — the
            # metadata travels via comments and tags instead.
            notes.append("table properties skipped (unsupported on pipeline views)")
        else:
            assignments = ", ".join(
                f"'{esc(key)}' = '{esc(value)}'" for key, value in properties.items()
            )
            spark.sql(f"ALTER TABLE {full_name} SET TBLPROPERTIES ({assignments})")

    # 3. Table tags (idempotent — overwrites). Views use ALTER VIEW.
    tags = table.get("tags") or {}
    if tags:
        assignments = ", ".join(
            f"'{esc(key)}' = '{esc(tag_value(value))}'" for key, value in tags.items()
        )
        verb = "ALTER VIEW" if is_view else "ALTER TABLE"
        spark.sql(f"{verb} {full_name} SET TAGS ({assignments})")

    # 4. Column tags always; column comments on real tables only (Databricks
    #    has no DDL for view column comments — the semantic column tags
    #    pii/semantic_type/ai_role are what carry the meaning on views).
    for column, colspec in (spec.get("columns") or {}).items():
        if obj_type == "VIEW":
            notes.append(
                f"{column}: column metadata skipped (unsupported on plain views)"
            )
            continue
        verb = "ALTER MATERIALIZED VIEW" if is_view else "ALTER TABLE"
        spark.sql(
            f"{verb} {full_name} ALTER COLUMN {column} "
            f"COMMENT '{esc(colspec['description'].strip())}'"
        )
        tag_assignments = ", ".join(
            f"'{esc(field)}' = '{esc(tag_value(colspec[field]))}'"
            for field in COLUMN_TAG_FIELDS
        )
        spark.sql(
            f"{verb} {full_name} ALTER COLUMN {column} SET TAGS ({tag_assignments})"
        )

    # 5-6. Informational PK / FK — real tables only (views do not accept
    #       ADD CONSTRAINT).
    constraint_results = []
    primary_key = table.get("primary_key") or []
    foreign_keys = table.get("foreign_keys") or []
    if is_view and (primary_key or foreign_keys):
        notes.append("PK/FK constraints skipped (unsupported on views)")
    if not is_view and primary_key:
        constraint_results.append(
            add_constraint(
                spark, full_name, f"pk_{table['name']}",
                f"PRIMARY KEY {sql_list(primary_key)}",
            )
        )
    if not is_view:
        for fk in foreign_keys:
            ref_layer = fk.get("ref_layer", layer)
            ref_location = layer_map.get(ref_layer)
            if not ref_location:
                print(
                    f"    WARN FK on {full_name}: no location known for layer "
                    f"'{ref_layer}' — pass it in --layer-schemas"
                )
                continue
            ref_full = f"{ref_location}.{fk['ref_table']}"
            ref_columns = fk.get("ref_columns") or fk["columns"]
            constraint_results.append(
                add_constraint(
                    spark, full_name, f"fk_{table['name']}__{fk['ref_table']}",
                    f"FOREIGN KEY {sql_list(fk['columns'])} "
                    f"REFERENCES {ref_full} {sql_list(ref_columns)}",
                )
            )

    constraint_summary = ""
    if constraint_results:
        added = constraint_results.count("added")
        present = constraint_results.count("already-present")
        constraint_summary = f", constraints: {added} added / {present} already present"
    if notes:
        constraint_summary += " | " + "; ".join(notes)
    return constraint_summary


def main():
    parser = argparse.ArgumentParser(
        description="Apply metadata YAML to Unity Catalog (idempotent)."
    )
    parser.add_argument("--layer", required=True, choices=LAYERS)
    parser.add_argument("--catalog", required=True, help="UC catalog of this layer")
    parser.add_argument("--schema", required=True, help="UC schema of this layer")
    parser.add_argument(
        "--metadata-root", required=True,
        help="Path to the metadata/ folder in the deployed bundle",
    )
    parser.add_argument(
        "--layer-schemas", required=True,
        help="bronze=<catalog>.<schema>,silver=...,gold=... — used to resolve FK "
             "references across layers",
    )
    args = parser.parse_args()

    try:
        layer_map = parse_layer_schemas(args.layer_schemas)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        return 2

    own_location = f"{args.catalog}.{args.schema}"
    if layer_map.get(args.layer) and layer_map[args.layer] != own_location:
        print(
            f"WARN: --layer-schemas maps {args.layer} to "
            f"{layer_map[args.layer]} but --catalog/--schema is {own_location}; "
            "using the CLI values"
        )
    layer_map[args.layer] = own_location

    specs, errors = load_specs(args.metadata_root, args.layer)
    if errors:
        print("Invalid metadata YAML — fix these before deploying:")
        for error in errors:
            print(f"  - {error}")
        return 2
    if not specs:
        print(f"ERROR: no metadata YAML files found in {args.metadata_root}/{args.layer}")
        return 2

    # Import here so YAML-only failures never need a Spark session.
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    # Every documented table must already exist (apply runs after the layer task).
    missing = []
    object_types = {}
    for _, spec in specs:
        name = spec["table"]["name"]
        full_name = f"{own_location}.{name}"
        try:
            spark.sql(f"DESCRIBE TABLE {full_name}")
        except Exception:  # noqa: BLE001
            missing.append(full_name)
            continue
        rows = spark.sql(
            f"SELECT table_type FROM {args.catalog}.information_schema.tables "
            f"WHERE table_schema = '{esc(args.schema)}' AND table_name = '{esc(name)}'"
        ).collect()
        object_types[name] = rows[0]["table_type"] if rows else "UNKNOWN"
    if missing:
        print("ERROR: documented tables not found in Unity Catalog:")
        for name in missing:
            print(f"  - {name}")
        print("This task must run AFTER the layer's pipeline task has created them.")
        return 3

    print(f"Applying metadata for layer '{args.layer}' to {own_location}...")
    for path, spec in specs:
        name = spec["table"]["name"]
        full_name = f"{own_location}.{name}"
        obj_type = object_types.get(name, "UNKNOWN")
        summary = apply_one_table(spark, spec, full_name, args.layer, layer_map, obj_type)
        print(f"  {full_name} [{obj_type}]: comments + tags applied{summary}")

    print(f"Done: metadata applied to {len(specs)} table(s) in {own_location}.")
    return 0


if __name__ == "__main__":
    # Exit non-zero only on failure — this script may run inside an
    # IPython-style REPL where SystemExit(0) marks the task as failed.
    _exit_code = main()
    if _exit_code:
        sys.exit(_exit_code)
