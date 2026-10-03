# openFDA Drug Events Pipeline — Project Scope (Phase 0)

## Overview

A medallion architecture pipeline (Bronze → Silver → Gold) built on Unity Catalog
that ingests openFDA drug adverse event data and produces business-ready
aggregations for pharmacovigilance analysis. Includes a full metadata-as-code
layer (per-table YAML descriptions, AI roles, PII flags, PK/FK, data contracts)
applied to Unity Catalog and exported as a portable JSON data dictionary.

## Data Source

- **API:** openFDA Drug Adverse Events API (`https://api.fda.gov/drug/event.json`)
- **Apify Actor:** `scrupulous_waterbird_m4w/openfda-drug-events` (wrapper around openFDA API)
- **Daily limit:** 25,000 pulls (Apify free tier)
- **Initial ingestion:** 10,000 records (within daily limit)
- **Note:** Serverless compute in this workspace cannot resolve external DNS.
  Data is generated synthetically matching the exact openFDA API schema.
  Replace with live API calls when running on compute with internet access.

## Architecture

```
openfda_drug_events_pipeline/
├── databricks.yml                          Bundle root — variables, includes, targets
├── resources/
│   ├── pipelines.yml                       3 SDP pipeline definitions (bronze, silver, gold)
│   └── orchestration_job.yml               9-task job: bronze→meta→silver→meta→gold→meta→validate→export→notify
├── 01_bronze_ingestion.py                  Bronze SDP — 10K events, ~30K drugs, ~40K reactions
├── 02_silver_transform.py                  Silver SDP — 4 cleansed/enriched tables
├── 03_gold_aggregation.py                  Gold SDP — 5 business aggregations
├── metadata/
│   ├── bronze/                             3 YAML files (events, drugs, reactions)
│   ├── silver/                             4 YAML files (events, drugs, reactions, enriched)
│   └── gold/                               5 YAML files (by_drug, by_country, reactions, seriousness, temporal)
├── src/metadata/
│   ├── apply_metadata.py                   Applies YAML metadata to UC (comments, tags, PK/FK)
│   ├── validate_metadata.py                Quality gate — 100% coverage required
│   └── export_metadata.py                  Exports JSON data dictionary + contracts to UC volume
├── tests/
│   └── test_metadata_yaml.py               Unit tests for YAML schema validation
├── notify_pipeline_status_notebook.py      Notification notebook (run_if: ALL_DONE)
└── PHASE_0_SCOPE.md                        This file
```

## UC Schemas (per target)

| Layer    | Dev Schema                    | Prod Schema            |
|----------|-------------------------------|------------------------|
| Bronze   | workspace.openfda_bronze_dev  | workspace.openfda_bronze |
| Silver   | workspace.openfda_silver_dev | workspace.openfda_silver|
| Gold     | workspace.openfda_gold_dev    | workspace.openfda_gold  |
| Metadata | workspace.openfda_metadata_dev| workspace.openfda_metadata|

## Tables (12 total)

### Bronze (3 tables, ~80,000 rows)
- **events_bronze** (10,000 rows) — Report-level metadata: safetyreportid, dates, country, seriousness flags, patient demographics
- **drugs_bronze** (~30,000 rows) — Drug array exploded: drug name, characterization, indication, route, manufacturer
- **reactions_bronze** (~40,000 rows) — Reaction array exploded: MedDRA term, outcome code

### Silver (4 tables)
- **events_silver** — Cleansed reports: typed dates, sex/reporter/seriousness mapped to labels, derived booleans
- **drugs_silver** — Cleansed drugs: characterization mapped, route mapped, names normalized
- **reactions_silver** — Cleansed reactions: outcome mapped to text, fatal flag derived
- **events_enriched_silver** — Wide table: events + suspect drug + primary reaction

### Gold (5 tables)
- **gold_events_by_drug** — Events, seriousness rate, fatal rate, hospitalization rate by drug
- **gold_events_by_country** — Events, seriousness, top drug by country
- **gold_reactions_summary** — Reaction frequency, outcome distribution, fatal/recovery rates
- **gold_seriousness_analysis** — Seriousness by category, sex, age group
- **gold_temporal_trends** — Monthly event counts, seriousness trends

## Data Coverage

- 10,000 safety reports
- 20 distinct drugs (Lipitor, Metformin, Keytruda, Humira, etc.)
- 44 distinct MedDRA reaction terms (Nausea, Death, Cardiac arrest, etc.)
- 20 countries (US dominant at ~30%)
- ~25% serious, ~4% fatal within serious
- 36 months of temporal data (2022-01 through 2024-12)

## Job Chain (9 tasks)

```
bronze_task → apply_metadata_bronze → silver_task → apply_metadata_silver
→ gold_task → apply_metadata_gold → validate_metadata → export_metadata → notify
```

## Build Order

1. **Phase 1:** Create UC schemas + project structure (DONE)
2. **Phase 2:** Build bronze SDP pipeline (DONE)
3. **Phase 3:** Build silver SDP pipeline (DONE)
4. **Phase 4:** Build gold SDP pipeline (DONE)
5. **Phase 5:** Write metadata YAMLs (DONE)
6. **Phase 6:** Copy metadata scripts + notify (DONE)
7. **Phase 7:** Deploy bundle + run job (TODO)
8. **Phase 8:** Create Lakeview dashboard (FUTURE)
