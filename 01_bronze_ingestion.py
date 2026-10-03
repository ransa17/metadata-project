# ---------------------------------------------------------------------------
# BRONZE LAYER — Raw Ingestion (Spark Declarative Pipeline)
# ---------------------------------------------------------------------------
# Generates 10,000 synthetic openFDA drug adverse event records that
# match the real openFDA API schema (api.fda.gov/drug/event.json).
# Each safety report has a patient, 1-5 drugs, and 1-6 reactions.
#
# Tables created (in the pipeline's configured catalog.target schema):
#   - events_bronze     (10,000 report-level rows)
#   - drugs_bronze      (~30,000 drug rows — exploded from events)
#   - reactions_bronze  (~40,000 reaction rows — exploded from events)
# ---------------------------------------------------------------------------

import dlt
import random
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
)

random.seed(2024)

# ---------------------------------------------------------------------------
# Reference data — real drug names, indications, MedDRA terms, countries
# ---------------------------------------------------------------------------

_DRUGS = [
    ("Lipitor",        "Atorvastatin",     "Hypercholesterolemia",  "Pfizer",        "Oral"),
    ("Metformin",       "Metformin",        "Type 2 Diabetes",       "Merck",          "Oral"),
    ("Losartan",        "Losartan",         "Hypertension",         "Teva",           "Oral"),
    ("Omeprazole",     "Omeprazole",       "GERD",                  "AstraZeneca",   "Oral"),
    ("Sertraline",     "Sertraline",       "Depression",            "Pfizer",        "Oral"),
    ("Adderall",        "Amphetamine",     "ADHD",                  "Teva",           "Oral"),
    ("Oxycontin",      "Oxycodone",        "Pain",                  "Purdue",         "Oral"),
    ("Humira",          "Adalimumab",      "Rheumatoid Arthritis",  "AbbVie",         "Subcutaneous"),
    ("Keytruda",       "Pembrolizumab",    "Melanoma",              "Merck",          "Intravenous"),
    ("Eliquis",         "Apixaban",        "Atrial Fibrillation",   "Bristol Myers",  "Oral"),
    ("Januvia",        "Sitagliptin",     "Type 2 Diabetes",       "Merck",          "Oral"),
    ("Lyrica",          "Pregabalin",      "Fibromyalgia",          "Pfizer",        "Oral"),
    ("Crestor",        "Rosuvastatin",     "Hypercholesterolemia",  "AstraZeneca",   "Oral"),
    ("Advair",         "Fluticasone",     "Asthma",                "GSK",            "Inhalation"),
    ("Plavix",          "Clopidogrel",     "Thrombosis",            "Bristol Myers",  "Oral"),
    ("Abilify",         "Aripiprazole",    "Schizophrenia",         "Otsuka",         "Oral"),
    ("Symbicort",      "Budesonide",      "COPD",                  "AstraZeneca",   "Inhalation"),
    ("Trulicity",      "Dulaglutide",      "Type 2 Diabetes",       "Lilly",          "Subcutaneous"),
    ("Xarelto",        "Rivaroxaban",      "Thrombosis",            "Janssen",        "Oral"),
    ("Tamiflu",        "Oseltamivir",      "Influenza",             "Roche",          "Oral"),
]

_REACTIONS = [
    "Nausea", "Headache", "Dizziness", "Fatigue", "Vomiting",
    "Diarrhoea", "Abdominal pain", "Pruritus", "Rash", "Pyrexia",
    "Dyspnoea", "Chest pain", "Asthenia", "Insomnia", "Anxiety",
    "Constipation", "Somnolence", "Decreased appetite", "Arthralgia",
    "Back pain", "Myalgia", "Cough", "Hypertension", "Hypotension",
    "Tachycardia", "Bradycardia", "Oedema peripheral", "Pain in extremity",
    "Fall", "Syncope", "Death", "Cardiac arrest", "Myocardial infarction",
    "Stroke", "Hepatic failure", "Renal failure", "Anaphylactic reaction",
    "Stevens-Johnson syndrome", "Seizure", "Loss of consciousness",
    "Gastrointestinal haemorrhage", "Pneumonia", "Sepsis", "Acute kidney injury",
]

_COUNTRIES = [
    "US", "GB", "DE", "FR", "JP", "CA", "IT", "ES", "NL", "AU",
    "BR", "IN", "CN", "MX", "BE", "SE", "CH", "PL", "TR", "KR",
]

_REPORTER_TYPES = [
    ("1", "Physician"), ("2", "Pharmacist"),
    ("3", "Other health professional"), ("4", "Lawyer"),
    ("5", "Consumer or non-health professional"),
]

_ROUTES = {"Oral": "048", "Subcutaneous": "065", "Intravenous": "063", "Inhalation": "076"}
_OUTCOMES = ["1", "2", "3", "4", "5", "6"]

# ---------------------------------------------------------------------------
# Generate 10,000 safety reports
# ---------------------------------------------------------------------------

_total_events = 10000
_events_data = []
_drugs_data = []
_reactions_data = []

for i in range(1, _total_events + 1):
    report_id = f"US-FDA-{i:08d}"

    # Dates — random within last 3 years
    base_date = datetime(2022, 1, 1)
    received = base_date + timedelta(days=random.randint(0, 1095))
    receivedate = received.strftime("%Y%m%d")
    receiptdate = (received + timedelta(days=random.randint(0, 30))).strftime("%Y%m%d")
    transmissiondate = (received + timedelta(days=random.randint(0, 10))).strftime("%Y%m%d")

    country = random.choices(
        _COUNTRIES, weights=[30, 10, 8, 8, 5, 5, 4, 4, 3, 3, 3, 3, 3, 3, 2, 2, 2, 1, 1, 1]
    )[0]

    reporter_code, reporter_label = random.choices(
        _REPORTER_TYPES, weights=[40, 10, 15, 2, 33]
    )[0]
    report_type = random.choices(["1", "2", "3"], weights=[70, 5, 25])[0]

    # Seriousness — ~25% serious
    is_serious = random.random() < 0.25
    serious = "1" if is_serious else "2"
    seriousness_death = "1" if (is_serious and random.random() < 0.15) else ""
    seriousness_life = "1" if (is_serious and random.random() < 0.20) else ""
    seriousness_hosp = "1" if (is_serious and random.random() < 0.45) else ""
    seriousness_disabling = "1" if (is_serious and random.random() < 0.10) else ""
    seriousness_congenital = "1" if (is_serious and random.random() < 0.05) else ""
    seriousness_other = "1" if (is_serious and random.random() < 0.30) else ""

    # Patient demographics
    age = random.randint(1, 95)
    age_unit = "1"  # years
    sex = random.choices(["1", "2", "0"], weights=[45, 50, 5])[0]
    weight = round(random.uniform(40, 150), 1) if random.random() > 0.15 else ""

    _events_data.append((
        report_id, receivedate, receiptdate, transmissiondate,
        country, reporter_code, country, report_type,
        serious, seriousness_death, seriousness_life,
        seriousness_hosp, seriousness_disabling, seriousness_congenital,
        seriousness_other,
        str(age), age_unit, sex, str(weight),
    ))

    # Drugs — 1 to 5 per report
    num_drugs = random.choices([1, 2, 3, 4, 5], weights=[20, 35, 25, 12, 8])[0]
    selected_drugs = random.sample(_DRUGS, min(num_drugs, len(_DRUGS)))
    for j, (brand, generic, indication, manufacturer, route) in enumerate(selected_drugs):
        char_code = "1" if j == 0 else ("2" if random.random() > 0.5 else "3")
        route_code = _ROUTES.get(route, "048")
        auth_num = f"{brand[:3].upper()}{random.randint(100, 999)}"
        _drugs_data.append((
            report_id, char_code, brand, generic, indication,
            route_code, auth_num, manufacturer, route, generic,
        ))

    # Reactions — 1 to 6 per report
    num_reactions = random.choices([1, 2, 3, 4, 5, 6], weights=[15, 25, 25, 20, 10, 5])[0]
    selected_reactions = random.sample(_REACTIONS, min(num_reactions, len(_REACTIONS)))
    for reaction_term in selected_reactions:
        # Serious reports have higher chance of severe outcomes
        if is_serious:
            outcome = random.choices(_OUTCOMES, weights=[10, 15, 25, 5, 15, 30])[0]
        else:
            outcome = random.choices(_OUTCOMES, weights=[30, 25, 15, 5, 2, 23])[0]
        _reactions_data.append((report_id, reaction_term, outcome))


# ---------------------------------------------------------------------------
# Bronze tables
# ---------------------------------------------------------------------------

@dlt.table(
    comment="Raw drug adverse event reports — one row per safety report, as-is from openFDA API.",
    table_properties={"layer": "bronze", "source": "openfda_api"},
)
@dlt.expect("safetyreportid is not null", "safetyreportid IS NOT NULL")
def events_bronze():
    """Landing zone for raw adverse event report metadata."""
    schema = StructType([
        StructField("safetyreportid",            StringType(), False),
        StructField("receivedate",                StringType(), True),
        StructField("receiptdate",                StringType(), True),
        StructField("transmissiondate",           StringType(), True),
        StructField("occurcountry",               StringType(), True),
        StructField("primarysourcequalification", StringType(), True),
        StructField("reportercountry",            StringType(), True),
        StructField("reporttype",                 StringType(), True),
        StructField("serious",                    StringType(), True),
        StructField("seriousnessdeath",            StringType(), True),
        StructField("seriousnesslifethreatening",  StringType(), True),
        StructField("seriousnesshospitalization", StringType(), True),
        StructField("seriousnessdisabling",       StringType(), True),
        StructField("seriousnesscongenitalanomali", StringType(), True),
        StructField("seriousnessother",           StringType(), True),
        StructField("patientonsetage",            StringType(), True),
        StructField("patientonsetageunit",        StringType(), True),
        StructField("patientsex",                  StringType(), True),
        StructField("patientweight",               StringType(), True),
    ])
    return spark.createDataFrame(_events_data, schema)


@dlt.table(
    comment="Raw drug records exploded from adverse event reports — one row per drug per report.",
    table_properties={"layer": "bronze", "source": "openfda_api"},
)
@dlt.expect("safetyreportid is not null", "safetyreportid IS NOT NULL")
def drugs_bronze():
    """Exploded drug array from patient.drug in openFDA records."""
    schema = StructType([
        StructField("safetyreportid",         StringType(), False),
        StructField("drugcharacterization",   StringType(), True),
        StructField("medicinalproduct",        StringType(), True),
        StructField("generic_name",           StringType(), True),
        StructField("drugindication",          StringType(), True),
        StructField("drugadministrationroute", StringType(), True),
        StructField("drugauthorizationnumb",   StringType(), True),
        StructField("manufacturer_name",      StringType(), True),
        StructField("route",                  StringType(), True),
        StructField("substance_name",         StringType(), True),
    ])
    return spark.createDataFrame(_drugs_data, schema)


@dlt.table(
    comment="Raw reaction records exploded from adverse event reports — one row per reaction per report.",
    table_properties={"layer": "bronze", "source": "openfda_api"},
)
@dlt.expect("safetyreportid is not null", "safetyreportid IS NOT NULL")
def reactions_bronze():
    """Exploded reaction array from patient.reaction in openFDA records."""
    schema = StructType([
        StructField("safetyreportid",    StringType(), False),
        StructField("reactionmeddrapt",   StringType(), True),
        StructField("reactionoutcome",   StringType(), True),
    ])
    return spark.createDataFrame(_reactions_data, schema)
