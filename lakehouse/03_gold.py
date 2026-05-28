import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/default-java"

from pyspark.sql import SparkSession, Window
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, when, avg, round as spark_round, lag

print("⏳ Menjalankan Gold Layer (Aggregasi)...")

builder = SparkSession.builder.appName("Gold-AirQuality") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder, extra_packages=["io.delta:delta-spark_2.12:3.1.0"]).getOrCreate()

# Baca data bersih dari Silver
silver = spark.read.format("delta").load("./lakehouse_data/silver/airquality")

# ==========================================
# 1. REPRO 1: Distribusi Kategori AQI (gold/aqi_category_dist)
# ==========================================
gold_category = silver.withColumn("kategori_aqi", 
    when(col("aqi") <= 50, "Baik")
    .when((col("aqi") > 50) & (col("aqi") <= 100), "Sedang")
    .when((col("aqi") > 100) & (col("aqi") <= 150), "Tidak Sehat")
    .when((col("aqi") > 150) & (col("aqi") <= 200), "Sangat Tidak Sehat")
    .otherwise("Berbahaya")
).groupBy("kategori_aqi").count()

gold_category.write.format("delta").mode("overwrite").save("./lakehouse_data/gold/aqi_category_dist")

# ==========================================
# 2. REPRO 2: Rata-rata per Jam (gold/aqi_hourly)
# ==========================================
gold_hourly = silver.groupBy("city", "jam").agg(
    spark_round(avg("aqi"), 2).alias("avg_aqi")
).orderBy("city", "jam")

gold_hourly.write.format("delta").mode("overwrite").save("./lakehouse_data/gold/aqi_hourly")

# ==========================================
# 3. ENHANCED: Tren AQI (gold/aqi_trend)
# ==========================================
window_spec = Window.partitionBy("city").orderBy("timestamp_wib")

gold_trend = silver \
    .withColumn("prev_aqi", lag("aqi", 1).over(window_spec)) \
    .withColumn("aqi_change", col("aqi") - col("prev_aqi")) \
    .withColumn("trend", when(col("aqi_change") > 5, "Memburuk")
                          .when(col("aqi_change") < -5, "Membaik")
                          .otherwise("Stabil")) \
    .groupBy("city", "trend").count()

gold_trend.write.format("delta").mode("overwrite").save("./lakehouse_data/gold/aqi_trend")

print("✅ Semua tabel Gold berhasil di-generate!")
gold_trend.show()