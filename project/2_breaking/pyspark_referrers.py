from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = (
    SparkSession.builder
    .appName("WikipediaClickstreamReferrers")
    .getOrCreate()
)

schema = "prev STRING, curr STRING, type STRING, clicks LONG"

df = (
    spark.read
    .option("sep", "\t")
    .option("header", "false")
    .schema(schema)
    .csv("data/clickstream-enwiki-2026-06.tsv")
)

results = (
    df.groupBy("prev")
    .agg(count("*").alias("row_count"))
    .orderBy(col("row_count").desc())
)

results.show(10, truncate=False)

print(f"Total rows read: {df.count():,}")
print(f"Unique referrers: {results.count():,}")

spark.stop()
