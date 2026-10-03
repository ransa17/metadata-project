# ---------------------------------------------------------------------------
# SILVER LAYER — Cleansed & Enriched (Spark Declarative Pipeline)
# ---------------------------------------------------------------------------
# Reads bronze tables across the pipeline boundary using catalog/schema
# values passed in via the pipeline's `configuration` block.
#
# Tables created:
#   - events_silver          (typed dates, normalized sex/country/seriousness)
#   - drugs_silver           (cleaned drug names, characterization mapped)
#   - reactions_silver        (outcome mapped to text, MedDRA terms cleaned)
#   - events_enriched_silver (events joined with suspect drug + primary reaction)
# ---------------------------------------------------------------------------

import dlt
from pyspark.sql import functions as F

bronze_catalog = spark.conf.get("bronze.catalog")
bronze_schema = spark.conf.get("bronze.schema")
bronze_db = f"{bronze_catalog}.{bronze_schema}"


# ---------------------------------------------------------------------------
# Silver tables
# ---------------------------------------------------------------------------

@dlt.table(
    comment="Cleansed adverse event reports — typed dates, normalized fields, derived seriousness label.",
    table_properties={"layer": "silver"},
)
@dlt.expect("safetyreportid not null", "safetyreportid IS NOT NULL")
@dlt.expect("receive_date valid", "receive_date IS NOT NULL")
def events_silver():
    """Cleansed report-level data: parse YYYYMMDD dates, map codes to labels."""
    return (
        spark.table(f"{bronze_db}.events_bronze")
        .withColumn("receive_date", F.to_date(F.col("receivedate"), "yyyyMMdd"))
        .withColumn("receipt_date", F.to_date(F.col("receiptdate"), "yyyyMMdd"))
        .withColumn("transmission_date", F.to_date(F.col("transmissiondate"), "yyyyMMdd"))
        .withColumn("patient_age", F.col("patientonsetage").cast("int"))
        .withColumn("patient_weight_kg", F.col("patientweight").cast("double"))
        .withColumn("sex_label", F.when(F.col("patientsex") == "1", "Male")
                     .when(F.col("patientsex") == "2", "Female")
                     .otherwise("Unknown"))
        .withColumn("serious_label", F.when(F.col("serious") == "1", "Serious")
                     .otherwise("Non-Serious"))
        .withColumn("reporter_type", F.when(F.col("primarysourcequalification") == "1", "Physician")
                     .when(F.col("primarysourcequalification") == "2", "Pharmacist")
                     .when(F.col("primarysourcequalification") == "3", "Other Health Professional")
                     .when(F.col("primarysourcequalification") == "4", "Lawyer")
                     .when(F.col("primarysourcequalification") == "5", "Consumer")
                     .otherwise("Unknown"))
        .withColumn("is_fatal", F.when(F.col("seriousnessdeath") == "1", True).otherwise(False))
        .withColumn("is_hospitalized", F.when(F.col("seriousnesshospitalization") == "1", True).otherwise(False))
        .withColumn("is_life_threatening", F.when(F.col("seriousnesslifethreatening") == "1", True).otherwise(False))
        .withColumn("is_disabling", F.when(F.col("seriousnessdisabling") == "1", True).otherwise(False))
        .withColumn("seriousness_category",
                     F.when(F.col("seriousnessdeath") == "1", "Death")
                     .when(F.col("seriousnesslifethreatening") == "1", "Life-Threatening")
                     .when(F.col("seriousnesshospitalization") == "1", "Hospitalization")
                     .when(F.col("seriousnessdisabling") == "1", "Disabling")
                     .when(F.col("seriousnesscongenitalanomali") == "1", "Congenital Anomaly")
                     .when(F.col("seriousnessother") == "1", "Other Serious")
                     .otherwise("Non-Serious"))
        .select(
            "safetyreportid", "receive_date", "receipt_date", "transmission_date",
            "occurcountry", "reporter_type", "reportercountry", "reporttype",
            "serious", "serious_label", "seriousness_category",
            "is_fatal", "is_hospitalized", "is_life_threatening", "is_disabling",
            "patient_age", "patient_weight_kg", "sex_label",
        )
    )


@dlt.table(
    comment="Cleansed drug records — characterization mapped to labels, brand/generic cleaned.",
    table_properties={"layer": "silver"},
)
@dlt.expect("safetyreportid not null", "safetyreportid IS NOT NULL")
def drugs_silver():
    """Cleansed drug-level data: map characterization codes, normalize names."""
    return (
        spark.table(f"{bronze_db}.drugs_bronze")
        .withColumn("characterization", F.when(F.col("drugcharacterization") == "1", "Suspect")
                     .when(F.col("drugcharacterization") == "2", "Concomitant")
                     .when(F.col("drugcharacterization") == "3", "Interacting")
                     .otherwise("Unknown"))
        .withColumn("route_label", F.when(F.col("drugadministrationroute") == "048", "Oral")
                     .when(F.col("drugadministrationroute") == "063", "Intravenous")
                     .when(F.col("drugadministrationroute") == "065", "Subcutaneous")
                     .when(F.col("drugadministrationroute") == "076", "Inhalation")
                     .otherwise("Unknown"))
        .withColumn("drug_name", F.trim(F.initcap(F.col("medicinalproduct"))))
        .select(
            "safetyreportid", "characterization", "drug_name",
            "generic_name", "drugindication", "route_label",
            "drugauthorizationnumb", "manufacturer_name", "substance_name",
        )
    )


@dlt.table(
    comment="Cleansed reaction records — outcome mapped to human-readable labels.",
    table_properties={"layer": "silver"},
)
@dlt.expect("safetyreportid not null", "safetyreportid IS NOT NULL")
def reactions_silver():
    """Cleansed reaction-level data: map outcome codes to text."""
    return (
        spark.table(f"{bronze_db}.reactions_bronze")
        .withColumn("outcome_label",
                     F.when(F.col("reactionoutcome") == "1", "Recovered/Resolved")
                     .when(F.col("reactionoutcome") == "2", "Recovering/Resolving")
                     .when(F.col("reactionoutcome") == "3", "Not Recovered/Not Resolved")
                     .when(F.col("reactionoutcome") == "4", "Recovered with Sequelae")
                     .when(F.col("reactionoutcome") == "5", "Fatal")
                     .when(F.col("reactionoutcome") == "6", "Unknown")
                     .otherwise("Unknown"))
        .withColumn("is_fatal_reaction", F.when(F.col("reactionoutcome") == "5", True).otherwise(False))
        .select(
            "safetyreportid", "reactionmeddrapt", "reactionoutcome",
            "outcome_label", "is_fatal_reaction",
        )
    )


@dlt.table(
    comment="Enriched events — report joined with suspect drug and primary (first) reaction for wide analysis.",
    table_properties={"layer": "silver"},
)
def events_enriched_silver():
    """Wide table joining events with their suspect drug and primary reaction."""
    events = spark.table(f"{bronze_db}.events_bronze")
    drugs = spark.table(f"{bronze_db}.drugs_bronze")
    reactions = spark.table(f"{bronze_db}.reactions_bronze")

    # Get suspect drug (characterization=1) — first one if multiple
    suspect_drugs = (
        drugs
        .filter(F.col("drugcharacterization") == "1")
        .withColumn("rn", F.row_number().over(
            F.Window.partitionBy("safetyreportid").orderBy("medicinalproduct")
        ))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # Get primary reaction (first one by MedDRA term)
    primary_reactions = (
        reactions
        .withColumn("rn", F.row_number().over(
            F.Window.partitionBy("safetyreportid").orderBy("reactionmeddrapt")
        ))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumnRenamed("reactionmeddrapt", "primary_reaction")
        .withColumnRenamed("reactionoutcome", "primary_outcome")
    )

    return (
        events
        .join(suspect_drugs, "safetyreportid", "left")
        .join(primary_reactions, "safetyreportid", "left")
        .withColumn("receive_date", F.to_date(F.col("receivedate"), "yyyyMMdd"))
        .withColumn("patient_age", F.col("patientonsetage").cast("int"))
        .withColumn("sex_label", F.when(F.col("patientsex") == "1", "Male")
                     .when(F.col("patientsex") == "2", "Female")
                     .otherwise("Unknown"))
        .withColumn("serious_label", F.when(F.col("serious") == "1", "Serious")
                     .otherwise("Non-Serious"))
        .select(
            "safetyreportid", "receive_date", "occurcountry",
            "serious_label", "patient_age", "sex_label",
            "medicinalproduct", "drugindication",
            "primary_reaction", "primary_outcome",
        )
    )
