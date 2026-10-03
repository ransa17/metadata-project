# ---------------------------------------------------------------------------
# Notification notebook — fires at the end of the orchestration job
# (run_if: ALL_DONE) regardless of upstream task outcomes.
# ---------------------------------------------------------------------------

import json
from datetime import datetime

print("=" * 60)
print("openFDA Drug Events Pipeline — Status Notification")
print("=" * 60)
print(f"Timestamp: {datetime.now().isoformat()}")

try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Check bronze tables
    bronze_catalog = spark.conf.get("bronze.catalog", "workspace")
    bronze_schema = spark.conf.get("bronze.schema", "openfda_bronze_dev")
    silver_schema = spark.conf.get("silver.schema", "openfda_silver_dev")
    gold_schema = spark.conf.get("gold.schema", "openfda_gold_dev")

    for layer_schema in [bronze_schema, silver_schema, gold_schema]:
        try:
            tables = spark.sql(f"SHOW TABLES IN workspace.{layer_schema}").collect()
            print(f"\\nLayer workspace.{layer_schema}: {len(tables)} tables")
            for t in tables:
                count = spark.sql(f"SELECT COUNT(*) as cnt FROM workspace.{layer_schema}.{t.tableName}").collect()[0].cnt
                print(f"  - {t.tableName}: {count} rows")
        except Exception as e:
            print(f"  Layer workspace.{layer_schema}: not available ({e})")

except Exception as e:
    print(f"Spark not available in notify context: {e}")

print("\nPipeline notification complete.")
