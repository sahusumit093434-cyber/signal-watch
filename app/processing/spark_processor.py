import logging
import os
import shutil
from datetime import datetime
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from app.config import (
    CLEANED_JSON_PATH,
    COMPANY_SCORES_JSON_PATH,
    COMPANY_SCORES_CSV_PATH,
    REFERENCE_DATE
)

logger = logging.getLogger(__name__)

def get_spark_session() -> SparkSession:
    """
    Creates or retrieves a local Spark session.
    Configures log level to reduce noise.
    """
    spark = SparkSession.builder \
        .appName("SignalWatchRiskScoring") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def process_risk_scores(reference_date=REFERENCE_DATE):
    """
    Runs the PySpark pipeline to:
    1. Load cleaned records applying explicit schema.
    2. Compute event-level risk scores with recency weights.
    3. Aggregate company risk scores using top 5 events.
    4. Write JSON and CSV outputs using Pandas to prevent winutils dependency on Windows.
    """
    if not os.path.exists(CLEANED_JSON_PATH):
        logger.error(f"Cleaned events JSON not found at {CLEANED_JSON_PATH}. Run ingestion first.")
        return None, None

    spark = get_spark_session()

    # Define explicit schema
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("company_name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("severity", IntegerType(), False),
        StructField("confidence", DoubleType(), False),
        StructField("published_at", StringType(), False),
        StructField("country", StringType(), True),
        StructField("source", StringType(), True),
        StructField("description", StringType(), True)
    ])

    # Load data
    df = spark.read.schema(schema).option("multiLine", "true").json(CLEANED_JSON_PATH)

    # 1. Compute recency weights
    ref_date_val = reference_date if reference_date else datetime.utcnow().strftime("%Y-%m-%d")
    df_age = df.withColumn("published_date", F.to_date(F.col("published_at"))) \
               .withColumn("ref_date", F.lit(ref_date_val).cast("date")) \
               .withColumn("age_days", F.datediff(F.col("ref_date"), F.col("published_date")))

    df_weight = df_age.withColumn(
        "recency_weight",
        F.when(F.col("age_days") <= 7, 1.0)
         .when((F.col("age_days") >= 8) & (F.col("age_days") <= 30), 0.8)
         .otherwise(0.6)
    )

    # 2. Compute event-level risk score
    # Event Risk Score = min(100, Severity * 20 * Confidence * Recency Weight)
    df_event_scores = df_weight.withColumn(
        "event_risk_score",
        F.round(
            F.least(
                F.lit(100.0),
                F.col("severity") * 20.0 * F.col("confidence") * F.col("recency_weight")
            ),
            2
        )
    )

    # Collect the enriched events as list of dicts for local memory querying in FastAPI
    df_event_scores_clean = df_event_scores.drop("published_date", "ref_date", "age_days")
    df_pandas = df_event_scores_clean.toPandas()
    enriched_events = df_pandas.where(df_pandas.notnull(), None).to_dict('records')

    # 3. Compute Company Risk Scores
    # Partition by company, order by event_risk_score descending
    window_spec = Window.partitionBy("company_name").orderBy(F.col("event_risk_score").desc())
    df_ranked = df_event_scores.withColumn("row_num", F.row_number().over(window_spec))

    # Take top 10 events
    df_top_10 = df_ranked.filter(F.col("row_num") <= 10)

    # Average of top 10 (or fewer)
    df_company_avg = df_top_10.groupBy("company_name").agg(
        F.round(F.mean("event_risk_score"), 2).alias("risk_score")
    )

    # Classify
    df_company_final = df_company_avg.withColumn(
        "risk_level",
        F.when(F.col("risk_score") < 40.00, "LOW")
         .when((F.col("risk_score") >= 40.00) & (F.col("risk_score") < 70.00), "MEDIUM")
         .otherwise("HIGH")
    )

    # Total event count per company
    df_total_counts = df_event_scores.groupBy("company_name").agg(
        F.count("*").alias("event_count")
    )

    # Top categories (up to 3) per company
    df_company_categories = df_event_scores.groupBy("company_name", "category").count()
    window_cat = Window.partitionBy("company_name").orderBy(F.col("count").desc())
    df_company_categories_ranked = df_company_categories.withColumn("rank", F.row_number().over(window_cat))
    df_top_categories = df_company_categories_ranked.filter(F.col("rank") <= 3).groupBy("company_name").agg(
        F.collect_list("category").alias("top_categories")
    )

    # Merge company stats
    df_company_complete = df_company_final.join(df_total_counts, "company_name", "left") \
                                          .join(df_top_categories, "company_name", "left")

    # Ensure parent directories exist
    os.makedirs(os.path.dirname(COMPANY_SCORES_JSON_PATH), exist_ok=True)

    df_comp_pandas = df_company_complete.toPandas()
    df_comp_pandas_clean = df_comp_pandas.where(df_comp_pandas.notnull(), None)
    company_scores = df_comp_pandas_clean.to_dict('records')
    
    # Convert list elements to standard Python lists
    for score in company_scores:
        if score.get("top_categories") is not None:
            score["top_categories"] = list(score["top_categories"])

    # Write files using Python built-ins and Pandas
    import json
    with open(COMPANY_SCORES_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(company_scores, f, indent=2)
        
    df_comp_pandas.to_csv(COMPANY_SCORES_CSV_PATH, index=False)

    return enriched_events, company_scores
