# openFDA Drug Events Pipeline

A medallion architecture pipeline for openFDA drug adverse event data with
AI-ready metadata-as-code layer.

## Quick Start

```bash
# Validate the bundle
databricks bundle validate -t dev

# Deploy to dev
databricks bundle deploy -t dev

# Run the orchestration job
databricks bundle run openfda_orchestration -t dev
```

## Architecture

Bronze (3 raw tables) → Silver (4 cleansed/enriched) → Gold (5 business aggregations)

Each layer is followed by a metadata apply task that writes table/column
descriptions, tags, and PK/FK constraints to Unity Catalog from per-table
YAML files. A validation gate enforces 100% metadata coverage, and an export
task writes a portable JSON data dictionary + per-table data contracts to a
UC volume.

## Data Source

openFDA Drug Adverse Events API via Apify actor
`scrupulous_waterbird_m4w/openfda-drug-events`. 10,000 records ingested
(within the 25,000/day Apify limit).

See [PHASE_0_SCOPE.md](PHASE_0_SCOPE.md) for full project specification.
