#!/usr/bin/env python3
"""Export a portable, machine-readable metadata package to a UC volume.

Runs as the final bundle task (after validate_metadata). Unity Catalog tags
and comments do NOT travel with Delta files when data is moved to another
platform, so this task writes a self-contained JSON package beside the data:

  <volume>/data_dictionary.json          — full dictionary, all layers/tables
  <volume>/contracts/<layer>__<table>.json — one data contract per table
  <volume>/_manifest.json                 — package index + timestamp

Each contract includes grain, owner, source, PK/FKs, quality rules,
business definitions, and per-column type/unit/examples/PII flag/semantic
type/AI role — enough for another platform, an LLM, or a text-to-SQL tool
to understand the dataset with no access to this workspace.

Idempotent: files are overwritten on every run.

Usage:

    python export_metadata.py \
        --metadata-root /Workspace/.../metadata \
        --layer-schemas bronze=...,silver=...,gold=... \
        --volume /Volumes/workspace/medallion_metadata/metadata_exports \
        --bundle-target prod

Exit codes: 0 = exported; 2 = invalid metadata YAML.
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import yaml

LAYERS = ("bronze", "silver", "gold")
CONTRACT_VERSION = "1.0"


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


def parse_volume(volume_path):
    """'/Volumes/<catalog>/<schema>/<volume>[/sub]' -> (catalog, schema, volume, subpath)."""
    parts = volume_path.rstrip("/").split("/")
    if len(parts) < 5 or parts[1] != "Volumes":
        raise ValueError(
            f"--volume must look like /Volumes/<catalog>/<schema>/<volume>, got {volume_path!r}"
        )
    catalog, schema, volume = parts[2], parts[3], parts[4]
    subpath = "/".join(parts[5:])
    return catalog, schema, volume, subpath


def write_text(path, content):
    """Write a UTF-8 file, falling back to dbutils.fs for odd volume mounts."""
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(content)
        return
    except OSError:
        pass
    from databricks.sdk.runtime import dbutils  # noqa: PLC0415

    dbutils.fs.put("file:" + path, content, overwrite=True)


def load_specs(metadata_root, layer):
    layer_dir = os.path.join(metadata_root, layer)
    if not os.path.isdir(layer_dir):
        return {}, [f"Metadata directory not found: {layer_dir}"]
    specs, errors = {}, []
    for fname in sorted(os.listdir(layer_dir)):
        if not fname.endswith(".yml"):
            continue
        path = os.path.join(layer_dir, fname)
        with open(path, "r", encoding="utf-8") as handle:
            spec = yaml.safe_load(handle)
        if not isinstance(spec, dict) or "table" not in spec:
            errors.append(f"{path}: missing top-level 'table' section")
            continue
        specs[spec["table"]["name"]] = spec
    return specs, errors


def build_table_entry(layer, location, table_name, spec, live_columns):
    """Build the dictionary/contract body for one table."""
    table = spec["table"]
    full_name = f"{location}.{table_name}"
    columns = []
    for column, colspec in (spec.get("columns") or {}).items():
        live = live_columns.get(column, {})
        columns.append(
            {
                "name": column,
                "type": live.get("data_type"),
                "nullable": live.get("is_nullable"),
                "description": colspec.get("description"),
                "unit": colspec.get("unit"),
                "examples": colspec.get("examples"),
                "pii": colspec.get("pii"),
                "semantic_type": colspec.get("semantic_type"),
                "ai_role": colspec.get("ai_role"),
            }
        )
    return full_name, {
        "name": table_name,
        "full_name": full_name,
        "layer": layer,
        "description": table.get("description"),
        "grain": table.get("grain"),
        "owner": table.get("owner"),
        "source_system": table.get("source_system"),
        "ingestion_method": table.get("ingestion_method"),
        "refresh_frequency": table.get("refresh_frequency"),
        "intended_use": table.get("intended_use"),
        "business_definitions": table.get("business_definitions"),
        "tags": table.get("tags"),
        "primary_key": table.get("primary_key") or [],
        "foreign_keys": table.get("foreign_keys") or [],
        "quality_rules": table.get("quality_rules") or [],
        "columns": columns,
    }


def main():
    parser = argparse.ArgumentParser(description="Export metadata package to a UC volume.")
    parser.add_argument("--metadata-root", required=True)
    parser.add_argument("--layer-schemas", required=True)
    parser.add_argument(
        "--volume", required=True,
        help="Export location, e.g. /Volumes/workspace/medallion_metadata/metadata_exports",
    )
    parser.add_argument("--bundle-target", default="", help="Bundle target name (dev/stage/prod)")
    args = parser.parse_args()

    try:
        layer_map = parse_layer_schemas(args.layer_schemas)
        vol_catalog, vol_schema, vol_name, subpath = parse_volume(args.volume)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        return 2

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    # Make sure the export volume exists (no-op if it already does).
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {vol_catalog}.{vol_schema}")
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {vol_catalog}.{vol_schema}.{vol_name}")
    except Exception as exc:  # noqa: BLE001 - creation may lack permissions
        print(
            f"WARN: could not create {vol_catalog}.{vol_schema}.{vol_name} "
            f"({exc}). Continuing — export will fail if the volume does not exist."
        )

    export_dir = args.volume.rstrip("/")
    generated_at = datetime.now(timezone.utc).isoformat()
    layers_out = {}
    contracts = []
    errors = []

    for layer in LAYERS:
        if layer not in layer_map:
            continue
        location = layer_map[layer]
        catalog, schema = location.split(".")
        specs, load_errors = load_specs(args.metadata_root, layer)
        errors.extend(load_errors)

        # Live physical column types for the contracts.
        live_columns = {}
        for row in spark.sql(
            f"SELECT table_name, column_name, data_type, is_nullable "
            f"FROM {catalog}.information_schema.columns "
            f"WHERE table_schema = '{esc(schema)}'"
        ).collect():
            row = row.asDict()
            live_columns.setdefault(row["table_name"], {})[row["column_name"]] = row

        tables_out = []
        for table_name, spec in sorted(specs.items()):
            full_name, entry = build_table_entry(
                layer, location, table_name, spec,
                live_columns.get(table_name, {}),
            )
            tables_out.append(entry)
            contracts.append((layer, full_name, entry))
        layers_out[layer] = {"location": location, "tables": tables_out}

    if errors:
        print("Invalid metadata YAML — not exporting:")
        for error in errors:
            print(f"  - {error}")
        return 2

    # ---- data dictionary --------------------------------------------------
    dictionary = {
        "package": "medallion_metadata_export",
        "package_version": CONTRACT_VERSION,
        "generated_at": generated_at,
        "bundle_target": args.bundle_target,
        "layers": layers_out,
    }
    dictionary_path = f"{export_dir}/data_dictionary.json"
    write_text(dictionary_path, json.dumps(dictionary, indent=2, ensure_ascii=False))

    # ---- one data contract per table --------------------------------------
    written = [dictionary_path]
    for layer, full_name, entry in contracts:
        contract = {
            "contract_version": CONTRACT_VERSION,
            "generated_at": generated_at,
            "bundle_target": args.bundle_target,
            "table": entry["full_name"],
            "layer": layer,
            "description": entry["description"],
            "grain": entry["grain"],
            "owner": entry["owner"],
            "source_system": entry["source_system"],
            "refresh_frequency": entry["refresh_frequency"],
            "keys": {
                "primary_key": entry["primary_key"],
                "foreign_keys": entry["foreign_keys"],
            },
            "business_definitions": entry["business_definitions"],
            "quality_rules": entry["quality_rules"],
            "tags": entry["tags"],
            "columns": entry["columns"],
        }
        table_only = full_name.split(".")[-1]
        contract_path = f"{export_dir}/contracts/{layer}__{table_only}.json"
        write_text(
            contract_path, json.dumps(contract, indent=2, ensure_ascii=False)
        )
        written.append(contract_path)

    # ---- manifest ---------------------------------------------------------
    manifest = {
        "package": "medallion_metadata_export",
        "package_version": CONTRACT_VERSION,
        "generated_at": generated_at,
        "bundle_target": args.bundle_target,
        "files": [
            path.replace(export_dir, "") for path in written
        ],
    }
    manifest_path = f"{export_dir}/_manifest.json"
    write_text(manifest_path, json.dumps(manifest, indent=2, ensure_ascii=False))
    written.append(manifest_path)

    print(f"Exported {len(contracts)} data contract(s) + data dictionary to {export_dir}:")
    for path in written:
        print(f"  {path}")
    return 0


if __name__ == "__main__":
    # Exit non-zero only on failure — this script may run inside an
    # IPython-style REPL where SystemExit(0) would mark the task as failed.
    _exit_code = main()
    if _exit_code:
        sys.exit(_exit_code)
