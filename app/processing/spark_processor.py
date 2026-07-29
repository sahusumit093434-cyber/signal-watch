import logging
import os
import shutil
from datetime import datetime
from app.config import (
    CLEANED_JSON_PATH,
    COMPANY_SCORES_JSON_PATH,
    COMPANY_SCORES_CSV_PATH,
    REFERENCE_DATE
)

logger = logging.getLogger(__name__)

def get_spark_session():
    """Creates a local Spark session."""
    from pyspark.sql import SparkSession
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

def _process_risk_scores_spark(reference_date):
    """Calculates risk scores using PySpark."""
    from pyspark.sql import Window
    import pyspark.sql.functions as F
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    
    spark = get_spark_session()

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

    df_event_scores_clean = df_event_scores.drop("published_date", "ref_date", "age_days")
    df_pandas = df_event_scores_clean.toPandas()
    enriched_events = df_pandas.where(df_pandas.notnull(), None).to_dict('records')

    # 3. Compute Company Risk Scores
    window_spec = Window.partitionBy("company_name").orderBy(F.col("event_risk_score").desc())
    df_ranked = df_event_scores.withColumn("row_num", F.row_number().over(window_spec))

    df_top_5 = df_ranked.filter(F.col("row_num") <= 5)

    df_company_avg = df_top_5.groupBy("company_name").agg(
        F.round(F.mean("event_risk_score"), 2).alias("risk_score")
    )

    df_company_final = df_company_avg.withColumn(
        "risk_level",
        F.when(F.col("risk_score") < 40.00, "LOW")
         .when((F.col("risk_score") >= 40.00) & (F.col("risk_score") < 70.00), "MEDIUM")
         .otherwise("HIGH")
    )

    df_total_counts = df_event_scores.groupBy("company_name").agg(
        F.count("*").alias("event_count")
    )

    df_company_categories = df_event_scores.groupBy("company_name", "category").count()
    window_cat = Window.partitionBy("company_name").orderBy(F.col("count").desc())
    df_company_categories_ranked = df_company_categories.withColumn("rank", F.row_number().over(window_cat))
    df_top_categories = df_company_categories_ranked.filter(F.col("rank") <= 3).groupBy("company_name").agg(
        F.collect_list("category").alias("top_categories")
    )

    df_company_complete = df_company_final.join(df_total_counts, "company_name", "left") \
                                          .join(df_top_categories, "company_name", "left")

    os.makedirs(os.path.dirname(COMPANY_SCORES_JSON_PATH), exist_ok=True)

    df_comp_pandas = df_company_complete.toPandas()
    df_comp_pandas_clean = df_comp_pandas.where(df_comp_pandas.notnull(), None)
    company_scores = df_comp_pandas_clean.to_dict('records')
    
    for score in company_scores:
        if score.get("top_categories") is not None:
            score["top_categories"] = list(score["top_categories"])

    import json
    with open(COMPANY_SCORES_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(company_scores, f, indent=2)
        
    df_comp_pandas.to_csv(COMPANY_SCORES_CSV_PATH, index=False)

    return enriched_events, company_scores

def _process_risk_scores_pandas(reference_date):
    """Graceful fallback calculation using pure Python & Pandas for Vercel/serverless environments."""
    import json
    import pandas as pd
    import numpy as np
    
    with open(CLEANED_JSON_PATH, "r", encoding="utf-8") as f:
        events = json.load(f)
        
    df = pd.DataFrame(events)
    if df.empty:
        return [], []
        
    # Calculate age in days
    ref_date_val = pd.to_datetime(reference_date if reference_date else datetime.utcnow().strftime("%Y-%m-%d"))
    df['published_date'] = pd.to_datetime(df['published_at'])
    df['age_days'] = (ref_date_val - df['published_date']).dt.days
    
    def get_weight(days):
        if pd.isna(days):
            return 0.6
        if days <= 7:
            return 1.0
        elif days <= 30:
            return 0.8
        else:
            return 0.6
            
    df['recency_weight'] = df['age_days'].apply(get_weight)
    
    # Event risk score
    df['event_risk_score'] = np.minimum(
        100.0,
        df['severity'].astype(float) * 20.0 * df['confidence'].astype(float) * df['recency_weight']
    )
    df['event_risk_score'] = df['event_risk_score'].round(2)
    
    df_clean = df.drop(columns=['published_date', 'age_days'])
    df_clean = df_clean.where(df_clean.notnull(), None)
    enriched_events = df_clean.to_dict('records')
    
    # Aggregate Company scores
    company_groups = df.groupby("company_name")
    company_scores = []
    
    for name, group in company_groups:
        top_5 = group.sort_values(by="event_risk_score", ascending=False).head(5)
        avg_score = round(float(top_5["event_risk_score"].mean()), 2)
        
        if avg_score < 40.00:
            level = "LOW"
        elif avg_score < 70.00:
            level = "MEDIUM"
        else:
            level = "HIGH"
            
        cat_counts = group["category"].value_counts()
        top_cats = list(cat_counts.head(3).index)
        total_count = int(group.shape[0])
        
        company_scores.append({
            "company_name": name,
            "risk_score": avg_score,
            "risk_level": level,
            "event_count": total_count,
            "top_categories": top_cats
        })
        
    os.makedirs(os.path.dirname(COMPANY_SCORES_JSON_PATH), exist_ok=True)
    with open(COMPANY_SCORES_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(company_scores, f, indent=2)
        
    df_out = pd.DataFrame(company_scores)
    df_out.to_csv(COMPANY_SCORES_CSV_PATH, index=False)
    
    return enriched_events, company_scores

def process_risk_scores(reference_date=REFERENCE_DATE):
    if not os.path.exists(CLEANED_JSON_PATH):
        logger.error(f"Cleaned events JSON not found at {CLEANED_JSON_PATH}. Run ingestion first.")
        return None, None
        
    try:
        return _process_risk_scores_spark(reference_date)
    except Exception as e:
        logger.warning(f"PySpark engine failed or JRE is not available ({e}). Falling back to pure Python/Pandas engine.")
        return _process_risk_scores_pandas(reference_date)
