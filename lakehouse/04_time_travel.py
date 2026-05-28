import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/default-java"

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql.functions import lit

print("⏳ Menyiapkan Mesin Waktu Delta Lake...")

builder = SparkSession.builder.appName("TimeTravel-AirQuality") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder, extra_packages=["io.delta:delta-spark_2.12:3.1.0"]).getOrCreate()

silver_path = "./lakehouse_data/silver/airquality"
deltaTable = DeltaTable.forPath(spark, silver_path)

# 1. Tampilkan history awal
print("\n=== 1. HISTORY TABEL SILVER (Awal) ===")
deltaTable.history().select("version", "timestamp", "operation").show(truncate=False)

# 2. MENGUBAH DATA
print("\n[MENGUBAH DATA] Mengupdate nama kota menjadi 'KOTA DARURAT'...")
deltaTable.update(
    condition="aqi > 0",
    set={"city": lit("KOTA DARURAT")}
)

# 3. PEMBUKTIAN DENGAN GROUP BY
print("\n=== DATA SEKARANG (Sesudah Update - Dihitung Per Kota) ===")
spark.read.format("delta").load(silver_path).groupBy("city").count().show()

print("\n=== DATA VERSI 0 (Sebelum Update - Menggunakan Time Travel) ===")
spark.read.format("delta").option("versionAsOf", 4).load(silver_path).groupBy("city").count().show()

# 4. Tampilkan history akhir
print("\n=== HISTORY TABEL SILVER (Akhir) ===")
deltaTable.history().select("version", "timestamp", "operation").show(truncate=False)

print("Owarimashitaツ Time Travel Sukses😪😹")