from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CheckParquet").getOrCreate()

df = spark.read.parquet("data/structured_table.parquet")

# show schema
df.printSchema()

# show some  record
df.show(10, truncate=False)

# show total record
print("Total records:", df.count())

