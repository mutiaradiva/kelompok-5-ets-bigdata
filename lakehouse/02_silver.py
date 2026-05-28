import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/default-java"

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, to_timestamp, from_utc_timestamp, date_format

print("⏳ Menjalankan Silver Layer (Cleaning)...")

builder = SparkSession.builder.appName("Silver-AirQuality") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder, extra_packages=["io.delta:delta-spark_2.12:3.1.0"]).getOrCreate()

# Baca dari Bronze
bronze_df = spark.read.format("delta").load("./lakehouse_data/bronze/airquality_api")

# Transformasi 1: Hapus Duplikat
silver_df = bronze_df.dropDuplicates(["city", "timestamp"])

# Transformasi 2: Filter Data Null & Cast Tipe Data Waktu ke WIB
silver_df = silver_df.filter(col("aqi").isNotNull()) \
    .withColumn("timestamp_wib", from_utc_timestamp(to_timestamp(col("timestamp")), "Asia/Jakarta"))

# Transformasi 3: Ekstrak Jam untuk analisis
silver_df = silver_df.withColumn("jam", date_format(col("timestamp_wib"), "HH:00"))

# Simpan ke Silver
silver_df.write.format("delta").mode("overwrite").save("./lakehouse_data/silver/airquality")

print("✅ Data bersih berhasil disimpan ke Silver Layer!")