import os
import logging
os.environ["JAVA_HOME"] = "/usr/lib/jvm/default-java"

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Membungkam log Spark yang berisik agar output bersih untuk screenshot
logging.getLogger("py4j").setLevel(logging.ERROR)

builder = SparkSession.builder.appName("IntipDataSemuaLayer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder, extra_packages=["io.delta:delta-spark_2.12:3.1.0"]).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("\n" + "="*60)
print(" 🥉 MENGINTIP BRONZE LAYER")
print("="*60)
# Cukup tampilkan 3 baris untuk bukti ada kolom _ingested_at
spark.read.format("delta").load("./lakehouse_data/bronze/airquality_api").show(3)


print("\n" + "="*60)
print(" 🥈 MENGINTIP SILVER LAYER")
print("="*60)
# Cukup tampilkan 3 baris untuk bukti zona waktu WIB dan kolom jam
spark.read.format("delta").load("./lakehouse_data/silver/airquality").show(3)


print("\n" + "="*60)
print(" 🥇 MENGINTIP GOLD LAYER")
print("="*60)

print("\n--- 1. REPRO 1: Distribusi Kategori AQI ---")
spark.read.format("delta").load("./lakehouse_data/gold/aqi_category_dist").show()

print("\n--- 2. REPRO 2: Rata-rata AQI per Jam ---")
# Dibatasi 5 saja agar terminal tidak kepanjangan
spark.read.format("delta").load("./lakehouse_data/gold/aqi_hourly").show(5)

print("\n--- 3. ENHANCED: Tren Kualitas Udara (Window Function) ---")
spark.read.format("delta").load("./lakehouse_data/gold/aqi_trend").show(5)

print("\nPengecekan seluruh layer selesai!")