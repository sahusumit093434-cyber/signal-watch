import pytest
from datetime import datetime, timedelta
import os
import json
import tempfile

from app.config import REFERENCE_DATE
from app.processing.spark_processor import get_spark_session
import pyspark.sql.functions as F

@pytest.fixture(scope="module")
def spark():
    return get_spark_session()

def create_df_from_json(spark, data_list, schema):
    """
    Creates a Spark DataFrame by writing raw Python data to a temporary JSON-Lines file
    and reading it back via the Spark JVM reader. This bypasses Python 3.14 pickling issues.
    """
    dicts = []
    for item in data_list:
        if isinstance(item, dict):
            dicts.append(item)
        else:
            dicts.append(dict(zip(schema, item)))
            
    # Create unique temp file path
    fd, temp_path = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    
    try:
        with open(temp_path, "w", encoding="utf-8") as f:
            for d in dicts:
                f.write(json.dumps(d) + "\n")
                
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
        
        type_map = {
            "company_name": StringType(),
            "severity": IntegerType(),
            "confidence": DoubleType(),
            "published_at": StringType(),
            "event_risk_score": DoubleType()
        }
        
        struct_fields = [StructField(name, type_map.get(name, StringType()), True) for name in schema]
        spark_schema = StructType(struct_fields)
        
        df = spark.read.schema(spark_schema).json(temp_path)
        # Force Spark to load the data before we delete the temp file
        df.cache()
        df.count()
        return df
    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass

def test_risk_score_calculation(spark):
    ref_date = datetime.strptime(REFERENCE_DATE, "%Y-%m-%d")
    
    # 1. Age <= 7 days: published_at = 2026-07-20 (4 days age -> weight 1.0)
    d1 = (ref_date - timedelta(days=4)).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # 2. Age 8-30 days: published_at = 2026-07-10 (14 days age -> weight 0.8)
    d2 = (ref_date - timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # 3. Age > 30 days: published_at = 2026-06-10 (44 days age -> weight 0.6)
    d3 = (ref_date - timedelta(days=44)).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # 4. Overflow check: Score capped at 100
    d4 = ref_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    data = [
        ("Company A", 3, 0.8, d1),
        ("Company B", 5, 0.9, d2),
        ("Company C", 4, 0.85, d3),
        ("Company D", 5, 1.0, d4)
    ]
    schema = ["company_name", "severity", "confidence", "published_at"]
    
    df = create_df_from_json(spark, data, schema)
    
    # Calculate age and recency weight
    df_age = df.withColumn("published_date", F.to_date(F.col("published_at"))) \
               .withColumn("ref_date", F.lit(REFERENCE_DATE).cast("date")) \
               .withColumn("age_days", F.datediff(F.col("ref_date"), F.col("published_date")))
               
    df_weight = df_age.withColumn(
        "recency_weight",
        F.when(F.col("age_days") <= 7, 1.0)
         .when((F.col("age_days") >= 8) & (F.col("age_days") <= 30), 0.8)
         .otherwise(0.6)
    )
    
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
    
    results = df_event_scores.toPandas().set_index("company_name")["event_risk_score"].to_dict()
    
    assert results["Company A"] == 48.0
    assert results["Company B"] == 72.0
    assert results["Company C"] == 40.8
    assert results["Company D"] == 100.0

def test_company_risk_score_averaging(spark):
    # Company with more than 5 events. Should average only the 5 highest.
    # Top 5: 90, 80, 70, 60, 50. Avg = 70.0. Level = HIGH.
    data = [
        ("MultiCorp", 90.0),
        ("MultiCorp", 80.0),
        ("MultiCorp", 70.0),
        ("MultiCorp", 60.0),
        ("MultiCorp", 50.0),
        ("MultiCorp", 40.0),
        ("MultiCorp", 30.0),
        
        # Company with fewer than 5 events. Should average all valid events.
        # Scores: 30, 40. Avg = 35.0. Level = LOW.
        ("FewCorp", 30.0),
        ("FewCorp", 40.0),
    ]
    schema = ["company_name", "event_risk_score"]
    
    df = create_df_from_json(spark, data, schema)
    
    from pyspark.sql import Window
    window_spec = Window.partitionBy("company_name").orderBy(F.col("event_risk_score").desc())
    df_ranked = df.withColumn("row_num", F.row_number().over(window_spec))
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
    
    results_raw = df_company_final.toPandas().set_index("company_name")[["risk_score", "risk_level"]].to_dict('index')
    results = {k: (v["risk_score"], v["risk_level"]) for k, v in results_raw.items()}
    
    assert results["MultiCorp"] == (70.0, "HIGH")
    assert results["FewCorp"] == (35.0, "LOW")
