"""Unit tests for the metadata YAML layer (metadata/<layer>/<table>.yml).

Pure pytest + PyYAML — no Spark session or Unity Catalog required. These
tests enforce the same schema rules that apply_metadata.py and
validate_metadata.py enforce at runtime, plus the expectation that every
table created by the medallion pipelines (bronze/silver/gold) has a metadata
file, so an undocumented table cannot slip into the repo unnoticed.

Run via the workspace test runner: tests/test_metadata_yaml.py
"""

from __future__ import annotations

import pathlib

import pytest
import yaml

# Every table the pipelines create — keep in sync with 01/02/03_*.py.
EXPECTED_TABLES = {
    "bronze": {"orders_bronze", "customers_bronze", "products_bronze"},
    "silver": {"orders_silver", "orders_enriched_silver"},
    "gold": {
        "gold_revenue_by_city",
        "gold_revenue_by_category",
        "gold_customer_spend",
    },
}

REQUIRED_TABLE_FIELDS = (
    "name", "layer", "description", "grain", "owner",
    "source_system", "refresh_frequency",
)
REQUIRED_TABLE_TAGS = ("layer", "source_system", "owner", "refresh_frequency")
REQUIRED_COLUMN_FIELDS = ("description", "pii", "semantic_type", "ai_role")
VALID_SEMANTIC_TYPES = {"identifier", "measure", "dimension", "timestamp"}
VALID_AI_ROLES = {"feature", "label", "key", "exclude"}


def _project_root() -> pathlib.Path:
    """Locate the bundle root (the folder containing databricks.yml)."""
    here = pathlib.Path(__file__).resolve()
    for candidate in here.parents:
        if (candidate / "databricks.yml").exists():
            return candidate
    pytest.fail("Could not locate project root (folder containing databricks.yml)")


def _load_all() -> dict[str, tuple[pathlib.Path, dict]]:
    """Load every metadata YAML as {table_name: (path, spec)}."""
    root = _project_root() / "metadata"
    loaded: dict[str, tuple[pathlib.Path, dict]] = {}
    for layer, expected in EXPECTED_TABLES.items():
        layer_dir = root / layer
        assert layer_dir.is_dir(), f"missing metadata folder: {layer_dir}"
        for yml in sorted(layer_dir.glob("*.yml")):
            spec = yaml.safe_load(yml.read_text(encoding="utf-8"))
            assert isinstance(spec, dict) and "table" in spec, f"{yml}: no 'table' section"
            table_name = spec["table"].get("name")
            assert table_name, f"{yml}: table.name missing"
            assert table_name not in loaded, f"duplicate metadata for table {table_name}"
            loaded[table_name] = (yml, spec)
    return loaded


ALL_METADATA = _load_all()


def test_every_pipeline_table_is_documented():
    """All 8 medallion tables have exactly one metadata YAML in the right layer."""
    documented = {
        name: (spec["table"].get("layer")) for name, (_, spec) in ALL_METADATA.items()
    }
    for layer, expected in EXPECTED_TABLES.items():
        in_layer = {name for name, doc_layer in documented.items() if doc_layer == layer}
        assert in_layer == expected, (
            f"{layer}: documented {sorted(in_layer)} != expected {sorted(expected)}"
        )


def test_filename_matches_table_name():
    """File name must be <table_name>.yml so tools can locate metadata by table."""
    for name, (path, _) in ALL_METADATA.items():
        assert path.name == f"{name}.yml", (
            f"{path.name} documents table '{name}' — rename to {name}.yml"
        )


def test_table_layer_matches_folder():
    """table.layer must match the folder (metadata/<layer>/...) the file lives in."""
    for _, (path, spec) in ALL_METADATA.items():
        layer_folder = path.parent.name
        assert spec["table"]["layer"] == layer_folder, (
            f"{path}: table.layer is {spec['table']['layer']!r} but file is in "
            f"metadata/{layer_folder}/"
        )


def test_required_table_fields_present():
    """Every table must carry description, grain, owner, source, refresh."""
    problems = []
    for name, (path, spec) in ALL_METADATA.items():
        table = spec["table"]
        for field in REQUIRED_TABLE_FIELDS:
            value = table.get(field)
            if not str(value or "").strip():
                problems.append(f"{path.name} [{name}]: table.{field} missing/empty")
    assert not problems, "\n".join(problems)


def test_required_table_tags_present():
    """The tags that the quality gate requires must all be declared in YAML."""
    problems = []
    for name, (path, spec) in ALL_METADATA.items():
        tags = spec["table"].get("tags") or {}
        for tag in REQUIRED_TABLE_TAGS:
            if not str(tags.get(tag) or "").strip():
                problems.append(
                    f"{path.name} [{name}]: required table tag '{tag}' missing"
                )
    assert not problems, "\n".join(problems)


def test_columns_present_with_required_fields():
    """Every column has description, pii (bool), semantic_type, ai_role."""
    problems = []
    for name, (path, spec) in ALL_METADATA.items():
        columns = spec.get("columns") or {}
        assert columns, f"{path.name} [{name}]: no columns documented"
        for column, colspec in columns.items():
            for field in REQUIRED_COLUMN_FIELDS:
                value = colspec.get(field)
                if field == "pii":
                    if not isinstance(value, bool):
                        problems.append(
                            f"{path.name} [{name}.{column}]: pii must be true/false"
                        )
                elif not str(value or "").strip():
                    problems.append(
                        f"{path.name} [{name}.{column}]: {field} missing/empty"
                    )
    assert not problems, "\n".join(problems)


def test_column_enum_values_valid():
    """semantic_type and ai_role use the controlled vocabularies."""
    problems = []
    for name, (path, spec) in ALL_METADATA.items():
        for column, colspec in (spec.get("columns") or {}).items():
            if colspec.get("semantic_type") not in VALID_SEMANTIC_TYPES:
                problems.append(
                    f"{path.name} [{name}.{column}]: semantic_type "
                    f"{colspec.get('semantic_type')!r} not in {sorted(VALID_SEMANTIC_TYPES)}"
                )
            if colspec.get("ai_role") not in VALID_AI_ROLES:
                problems.append(
                    f"{path.name} [{name}.{column}]: ai_role "
                    f"{colspec.get('ai_role')!r} not in {sorted(VALID_AI_ROLES)}"
                )
    assert not problems, "\n".join(problems)


def test_primary_key_columns_exist():
    """Every column listed in primary_key must be documented in columns."""
    problems = []
    for name, (path, spec) in ALL_METADATA.items():
        documented = set(spec.get("columns") or {})
        for key_column in spec["table"].get("primary_key") or []:
            if key_column not in documented:
                problems.append(
                    f"{path.name} [{name}]: primary key column '{key_column}' "
                    "is not documented under columns"
                )
    assert not problems, "\n".join(problems)


def test_foreign_key_columns_exist():
    """FK columns (local and referenced) must be documented, refs resolvable."""
    problems = []
    for name, (path, spec) in ALL_METADATA.items():
        documented = set(spec.get("columns") or {})
        for fk in spec["table"].get("foreign_keys") or []:
            for column in fk.get("columns", []):
                if column not in documented:
                    problems.append(
                        f"{path.name} [{name}]: FK column '{column}' not documented"
                    )
            ref_table = fk.get("ref_table")
            if ref_table not in ALL_METADATA:
                problems.append(
                    f"{path.name} [{name}]: FK references table '{ref_table}' "
                    "which has no metadata YAML"
                )
            else:
                _, ref_spec = ALL_METADATA[ref_table]
                ref_columns = set(ref_spec.get("columns") or {})
                for ref_column in fk.get("ref_columns") or fk.get("columns", []):
                    if ref_column not in ref_columns:
                        problems.append(
                            f"{path.name} [{name}]: FK references "
                            f"{ref_table}.{ref_column} which is not documented"
                        )
    assert not problems, "\n".join(problems)


def test_pii_flagged_columns_marked():
    """Sanity: the known PII columns in this pipeline are flagged pii: true."""
    for table, column in [
        ("customers_bronze", "customer_name"),
        ("customers_bronze", "email"),
        ("orders_enriched_silver", "customer_name"),
        ("gold_customer_spend", "customer_name"),
    ]:
        assert table in ALL_METADATA, f"missing metadata for {table}"
        colspec = ALL_METADATA[table][1]["columns"][column]
        assert colspec["pii"] is True, (
            f"{table}.{column} contains PII but is not flagged pii: true"
        )
        assert colspec["ai_role"] == "exclude", (
            f"{table}.{column} is PII and should have ai_role: exclude"
        )
