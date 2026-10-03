# ---------------------------------------------------------------------------
# GOLD LAYER — Business-Ready Aggregations (Spark Declarative Pipeline)
# ---------------------------------------------------------------------------
# Reads silver tables across the pipeline boundary using catalog/schema
# values passed in via the pipeline's `configuration` block.
#
# Tables created:
#   - gold_events_by_drug      (adverse event count, seriousness %, outcomes by drug)
#   - gold_events_by_country   (events, seriousness, top drugs by country)
#   - gold_reactions_summary    (reaction frequency, outcome distribution)
#   - gold_seriousness_analysis (serious vs non-serious by category and demographics)
#   - gold_temporal_trends      (monthly event counts, seriousness trends)
# ---------------------------------------------------------------------------

import dlt
from pyspark.sql import functions as F

silver_catalog = spark.conf.get("silver.catalog")
silver_schema = spark.conf.get("silver.schema")
silver_db = f"{silver_catalog}.{silver_schema}"


@dlt.table(
    comment="Adverse events aggregated by suspect drug — count, seriousness rate, fatal rate, top reactions.",
    table_properties={"layer": "gold"},
)
def gold_events_by_drug():
    """Aggregate events by suspect drug with seriousness metrics."""
    events = spark.table(f"{silver_db}.events_silver")
    drugs = spark.table(f"{silver_db}.drugs_silver")
    reactions = spark.table(f"{silver_db}.reactions_silver")

    suspect_drugs = drugs.filter(F.col("characterization") == "Suspect")

    # Join events with suspect drugs and reactions
    enriched = (
        events
        .join(suspect_drugs, "safetyreportid", "left")
        .join(reactions, "safetyreportid", "left")
    )

    return (
        enriched
        .groupBy("drug_name", "generic_name", "manufacturer_name", "drugindication")
        .agg(
            F.countDistinct("safetyreportid").alias("event_count"),
            F.sum(F.when(F.col("serious_label") == "Serious", 1).otherwise(0)).alias("serious_count"),
            F.sum(F.col("is_fatal").cast("int")).alias("fatal_count"),
            F.sum(F.col("is_hospitalized").cast("int")).alias("hospitalized_count"),
            F.sum(F.col("is_life_threatening").cast("int")).alias("life_threatening_count"),
            F.round(F.avg("patient_age"), 1).alias("avg_patient_age"),
            F.expr("percentile_approx(patient_age, 0.5)").alias("median_patient_age"),
        )
        .withColumn("seriousness_rate_pct", F.round(
            F.when(F.col("event_count") > 0, F.col("serious_count") * 100.0 / F.col("event_count"))
            .otherwise(0.0), 2))
        .withColumn("fatal_rate_pct", F.round(
            F.when(F.col("event_count") > 0, F.col("fatal_count") * 100.0 / F.col("event_count"))
            .otherwise(0.0), 2))
        .withColumn("hospitalization_rate_pct", F.round(
            F.when(F.col("event_count") > 0, F.col("hospitalized_count") * 100.0 / F.col("event_count"))
            .otherwise(0.0), 2))
        .orderBy(F.col("event_count").desc())
    )


@dlt.table(
    comment="Adverse events aggregated by occurrence country — counts, seriousness, top drug.",
    table_properties={"layer": "gold"},
)
def gold_events_by_country():
    """Aggregate events by country with seriousness breakdown."""
    events = spark.table(f"{silver_db}.events_silver")
    drugs = spark.table(f"{silver_db}.drugs_silver")

    suspect_drugs = drugs.filter(F.col("characterization") == "Suspect")

    # Get top drug per country using window
    drug_by_country = (
        events
        .join(suspect_drugs, "safetyreportid", "left")
        .groupBy("occurcountry", "drug_name")
        .agg(F.countDistinct("safetyreportid").alias("drug_event_count"))
        .withColumn("rn", F.row_number().over(
            F.Window.partitionBy("occurcountry").orderBy(F.col("drug_event_count").desc())
        ))
        .filter(F.col("rn") == 1)
        .select("occurcountry", F.col("drug_name").alias("top_drug"), F.col("drug_event_count").alias("top_drug_count"))
    )

    return (
        events
        .groupBy("occurcountry")
        .agg(
            F.countDistinct("safetyreportid").alias("event_count"),
            F.sum(F.when(F.col("serious_label") == "Serious", 1).otherwise(0)).alias("serious_count"),
            F.sum(F.col("is_fatal").cast("int")).alias("fatal_count"),
            F.round(F.avg("patient_age"), 1).alias("avg_patient_age"),
        )
        .join(drug_by_country, "occurcountry", "left")
        .withColumn("seriousness_rate_pct", F.round(
            F.when(F.col("event_count") > 0, F.col("serious_count") * 100.0 / F.col("event_count"))
            .otherwise(0.0), 2))
        .orderBy(F.col("event_count").desc())
    )


@dlt.table(
    comment="Reaction frequency and outcome distribution — top MedDRA terms by occurrence count.",
    table_properties={"layer": "gold"},
)
def gold_reactions_summary():
    """Aggregate reactions with outcome breakdown."""
    reactions = spark.table(f"{silver_db}.reactions_silver")

    return (
        reactions
        .groupBy("reactionmeddrapt")
        .agg(
            F.count("*").alias("occurrence_count"),
            F.sum(F.when(F.col("outcome_label") == "Fatal", 1).otherwise(0)).alias("fatal_count"),
            F.sum(F.when(F.col("outcome_label") == "Recovered/Resolved", 1).otherwise(0)).alias("recovered_count"),
            F.sum(F.when(F.col("outcome_label") == "Not Recovered/Not Resolved", 1).otherwise(0)).alias("not_recovered_count"),
            F.sum(F.when(F.col("outcome_label") == "Recovering/Resolving", 1).otherwise(0)).alias("recovering_count"),
            F.sum(F.when(F.col("outcome_label") == "Unknown", 1).otherwise(0)).alias("unknown_count"),
        )
        .withColumn("fatal_rate_pct", F.round(
            F.when(F.col("occurrence_count") > 0, F.col("fatal_count") * 100.0 / F.col("occurrence_count"))
            .otherwise(0.0), 2))
        .withColumn("recovery_rate_pct", F.round(
            F.when(F.col("occurrence_count") > 0,
                   (F.col("recovered_count") + F.col("recovering_count")) * 100.0 / F.col("occurrence_count"))
            .otherwise(0.0), 2))
        .orderBy(F.col("occurrence_count").desc())
    )


@dlt.table(
    comment="Seriousness analysis — breakdown by category, sex, and age group for safety profiling.",
    table_properties={"layer": "gold"},
)
def gold_seriousness_analysis():
    """Analyze seriousness patterns by demographic and category."""
    events = spark.table(f"{silver_db}.events_silver")

    return (
        events
        .withColumn("age_group",
                     F.when(F.col("patient_age") < 18, "Pediatric (0-17)")
                     .when(F.col("patient_age") < 40, "Adult (18-39)")
                     .when(F.col("patient_age") < 60, "Middle-aged (40-59)")
                     .when(F.col("patient_age") < 80, "Senior (60-79)")
                     .otherwise("Elderly (80+)")
        )
        .groupBy("seriousness_category", "sex_label", "age_group")
        .agg(
            F.countDistinct("safetyreportid").alias("event_count"),
            F.sum(F.col("is_fatal").cast("int")).alias("fatal_count"),
            F.round(F.avg("patient_age"), 1).alias("avg_age"),
            F.round(F.avg("patient_weight_kg"), 1).alias("avg_weight_kg"),
        )
        .orderBy(F.col("event_count").desc())
    )


@dlt.table(
    comment="Monthly temporal trends — event counts, seriousness, and fatal events over time.",
    table_properties={"layer": "gold"},
)
def gold_temporal_trends():
    """Aggregate events by month for trend analysis."""
    events = spark.table(f"{silver_db}.events_silver")

    return (
        events
        .withColumn("month", F.date_format("receive_date", "yyyy-MM"))
        .groupBy("month")
        .agg(
            F.countDistinct("safetyreportid").alias("event_count"),
            F.sum(F.when(F.col("serious_label") == "Serious", 1).otherwise(0)).alias("serious_count"),
            F.sum(F.col("is_fatal").cast("int")).alias("fatal_count"),
        )
        .withColumn("seriousness_rate_pct", F.round(
            F.when(F.col("event_count") > 0, F.col("serious_count") * 100.0 / F.col("event_count"))
            .otherwise(0.0), 2))
        .orderBy("month")
    )
