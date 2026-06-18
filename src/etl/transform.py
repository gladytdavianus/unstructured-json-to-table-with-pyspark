from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, length
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def transform_json_to_table(df: DataFrame) -> DataFrame:
    print("Transforming JSON to structured table...")
    
    total_before = df.count()
    
    if "review_id" not in df.columns:
        from pyspark.sql.functions import from_json
        schema = StructType([
            StructField("review_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("business_id", StringType(), True),
            StructField("stars", IntegerType(), True),
            StructField("text", StringType(), True),
            StructField("date", StringType(), True),
            StructField("useful", IntegerType(), True),
            StructField("funny", IntegerType(), True),
            StructField("cool", IntegerType(), True)
        ])
        df = df.withColumn("review", from_json(col("json"), schema))
        df = df.select("review.*")
    
    expected_columns = ["review_id", "user_id", "business_id", "stars", "text", "date", "useful", "funny", "cool"]
    for col_name in expected_columns:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None))
    
    df = df.withColumn("extracted_at", lit("2024-01-01"))
    df = df.withColumn("source", lit("yelp_academic_dataset"))
    
    total_after = df.count()
    print(f"✅ Transformed: {total_before} → {total_after} records")
    print(f"✅ Columns: {df.columns}")
    
    return df

def clean_data(df: DataFrame) -> DataFrame:
    print("Cleaning data...")
    
    total_before = df.count()
    
    df = df.filter(col("review_id").isNotNull())
    df = df.filter(col("text").isNotNull())
    df = df.filter(col("text") != "")
    df = df.withColumn("text_length", length(col("text")))
    df = df.filter(col("stars") >= 1)
    df = df.filter(col("stars") <= 5)
    
    total_after = df.count()
    removed = total_before - total_after
    print(f"✅ Cleaned: {total_before} → {total_after} records")
    print(f"   Removed: {removed} records (null/invalid)")
    
    return df
