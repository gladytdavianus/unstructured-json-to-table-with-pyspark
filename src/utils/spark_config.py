import os
from pyspark.sql import SparkSession

# Pastikan environment Java & Spark benar
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-11-openjdk-amd64'
os.environ['SPARK_HOME'] = os.path.expanduser('~/spark')

def get_spark_session(app_name="UnstructuredJSONToTable"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.jars.packages", "com.mysql:mysql-connector-j:8.0.33") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.localShuffling.enabled", "true") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.warehouse.dir", "data/spark-warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

def close_spark_session(spark):
    spark.stop()

