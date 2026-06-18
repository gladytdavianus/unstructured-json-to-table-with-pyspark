import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.spark_config import get_spark_session, close_spark_session
from src.utils.mysql_config import setup_mysql_database
from src.etl.extract import extract_json
from src.etl.transform import transform_json_to_table, clean_data
from src.etl.load import load_to_spark_table, load_to_parquet, load_to_csv, load_to_mysql

def main():
    print("\n" + "=" * 70)
    print("  UNSTRUCTURED JSON TO TABLE WITH PYSPARK")
    print("  Yelp Academic Dataset Review ETL Pipeline")
    print("=" * 70)

    # Path INPUT data
    json_path = "data/yelp_academic_dataset_review.json"

    # Path OUTPUT data
    parquet_output = "data/structured_table.parquet"
    csv_output = "data/structured_table.csv"

    spark_table = "yelp_reviews_table"
    mysql_table = "yelp_reviews"
    mysql_database = None
    sql_schema_path = "sql/schema_data.sql"

    print("\n[STEP 0/8] SETUP MySQL DATABASE")
    setup_mysql_database(sql_schema_path)

    print("\n[STEP 1/8] CREATE SPARK SESSION")
    spark = get_spark_session()
    print(f"Spark: {spark.version}")

    print("\n[STEP 2/8] EXTRACT JSON")
    print(f"Loading: {json_path}")
    df_raw = extract_json(spark, json_path)
    print(f"Total records loaded: {df_raw.count()}")

    print("\n[STEP 3/8] TRANSFORM")
    df_structured = transform_json_to_table(df_raw)
    df_structured.printSchema()

    print("\n[STEP 4/8] CLEAN")
    df_clean = clean_data(df_structured)

    # ✅ repartition for big dataset 
    df_clean = df_clean.repartition(200)

    total_records = df_clean.count()
    print(f"After cleaning: {total_records} records")

    print("\n[STEP 5/8] LOAD TO SPARK TABLE")
    load_to_spark_table(df_clean, spark_table, spark)

    print("\n[STEP 6/8] LOAD TO PARQUET")
    load_to_parquet(df_clean, parquet_output)

    print("\n[STEP 7/8] LOAD TO CSV + MySQL")
    load_to_csv(df_clean, csv_output)

    # ✅ batch write to MySQL
    load_to_mysql(df_clean, database=mysql_database, table=mysql_table, batchsize=5000)

    close_spark_session(spark)

    print("\n" + "=" * 70)
    print("  ETL COMPLETED!")
    print("=" * 70)
    print(f"\nFinal Output:")
    print(f"   - Input: {json_path}")
    print(f"   - Parquet: {parquet_output}")
    print(f"   - CSV: {csv_output}")
    print(f"   - Spark Table: {spark_table}")
    print(f"   - MySQL: {mysql_database or 'yelp_db'}.{mysql_table}")
    print(f"   - Total Records Processed: {total_records}")
    print(f"\nExpected: ~6.99 million records (full Yelp dataset)")
    print("=" * 70 + "\n")

if __name__ == "__main__":
    main()

