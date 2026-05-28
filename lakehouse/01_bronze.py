import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/default-java"

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import current_timestamp, lit

print("⏳ Menjalankan Bronze Layer...")

builder = SparkSession.builder.appName("Bronze-AirQuality") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")

spark = configure_spark_with_delta_pip(
    builder, extra_packages=["io.delta:delta-spark_2.12:3.1.0"]
).getOrCreate()

# 1. INGEST DATA API
api_df = spark.read.option("multiLine", True).json("hdfs://localhost:8020/data/airquality/api/")
bronze_api = api_df.withColumn("_ingested_at", current_timestamp()).withColumn("_source", lit("api"))
bronze_api.write.format("delta").mode("append").save("./lakehouse_data/bronze/airquality_api")
print("📸Data API berhasil disimpan ke Bronze Layer!")

# 2. INGEST DATA RSS
try:
    rss_df = spark.read.option("multiLine", True).json("hdfs://localhost:8020/data/airquality/rss/")
    bronze_rss = rss_df.withColumn("_ingested_at", current_timestamp()).withColumn("_source", lit("rss"))
    bronze_rss.write.format("delta").mode("append").save("./lakehouse_data/bronze/airquality_rss")
    print("🤤Data RSS berhasil disimpan ke Bronze Layer!")
except Exception as e:
    print(f"😞RSS kosong atau gagal ditarik: {e}")