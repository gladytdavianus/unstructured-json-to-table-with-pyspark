from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

def extract_json(spark: SparkSession, file_path: str) -> DataFrame:
    print(f"Extracting JSON from: {file_path}")
    print(f"File size: {file_path}")

    
    df = spark.read.json(file_path, multiLine=False)

    record_count = df.count()
    print(f"✅ Extracted {record_count} records")

    if record_count > 1000000:
        print(f"⚠️ Large dataset: {record_count} records (>1M)")

    return df

def extract_json_sample(file_path: str, limit: int = 10) -> list:
    import json
    samples = []
    with open(file_path, 'r') as f:
        for line in f:
            if line.strip():
                samples.append(json.loads(line))
                if len(samples) >= limit:
                    break
    return samples

