from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pathlib import Path
import os

# Load environment variables
load_dotenv(dotenv_path=Path(__file__).parent.parent.parent / ".env")

def load_to_spark_table(df: DataFrame, table_name: str, spark: SparkSession) -> None:
    print(f"Saving to Spark table: {table_name}")
    df.write.mode("overwrite").saveAsTable(table_name)
    print(f"Table created/overwritten: {table_name}")

def load_to_parquet(df: DataFrame, output_path: str = "data/structured_table.parquet") -> None:
    print(f"Saving to Parquet: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    print(f"Parquet saved: {output_path} ({df.count()} records)")

def load_to_csv(df: DataFrame, output_path: str = "data/structured_table.csv") -> None:
    print(f"Saving to CSV: {output_path}")
    df.write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"CSV saved: {output_path} ({df.count()} records)")

def load_to_mysql(df: DataFrame, database=None, table="yelp_reviews", batchsize=5000) -> None:
    """
    Load DataFrame to MySQL with JDBC batch write.
    """
    host = os.getenv("MYSQL_HOST", "localhost")
    user = os.getenv("MYSQL_USER", "root")
    password = os.getenv("MYSQL_PASSWORD", "")
    database = database or os.getenv("MYSQL_DATABASE", "yelp_db")

    print(f"Saving to MySQL: {database}.{table}")
    jdbc_url = f"jdbc:mysql://{host}:3306/{database}"

    (
        df.write
        .mode("overwrite")
        .format("jdbc")
        .option("url", jdbc_url)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("batchsize", str(batchsize))
        .save()
    )

    print(f"Data loaded to MySQL (spark-jdbc): {database}.{table} ({df.count()} records)")

