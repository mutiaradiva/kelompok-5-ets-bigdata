# [Ananda Fitri Wibowo]: Setup Spark & Read Local Data
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/default-java"

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, when, hour, to_timestamp, avg, count, round as spark_round, row_number
import json
import pandas as pd
from datetime import datetime

print("⏳ Memulai mesin Spark...")
spark = SparkSession.builder \
    .appName("AirQualityAnalysis") \
    .getOrCreate()

# MEMBACA LANGSUNG DARI FOLDER LOKAL (Bypass HDFS yang error)
print("📥 Membaca data API dari folder lokal...")
path_lokal = "dashboard/data/live_api.json"
df_api = spark.read.option("multiLine", True).json(path_lokal)
df_api.createOrReplaceTempView("air_quality")

# ==========================================
# ANALISIS 1: Distribusi Kategori AQI
# ==========================================
query_distribusi = """
WITH KategoriData AS (
    SELECT city,
        CASE 
            WHEN aqi <= 50 THEN 'Baik'
            WHEN aqi > 50 AND aqi <= 100 THEN 'Sedang'
            WHEN aqi > 100 AND aqi <= 150 THEN 'Tidak Sehat'
            WHEN aqi > 150 AND aqi <= 200 THEN 'Sangat Tidak Sehat'
            ELSE 'Berbahaya'
        END as kategori_aqi
    FROM air_quality
)
SELECT kategori_aqi, COUNT(*) as jumlah_event 
FROM KategoriData 
GROUP BY kategori_aqi
"""
distribusi_df = spark.sql(query_distribusi)
print("\n=== Analisis 1: Distribusi Kategori AQI ===")
distribusi_df.show()

# ==========================================
# ANALISIS 2: Rata-rata AQI per Jam
# ==========================================
query_tren_jam = """
    SELECT city, HOUR(CAST(timestamp AS TIMESTAMP)) as hour, ROUND(AVG(aqi), 2) as avg_aqi
    FROM air_quality 
    GROUP BY city, hour 
    ORDER BY city, hour ASC
"""
tren_jam_df = spark.sql(query_tren_jam)
print("\n=== Analisis 2: Rata-rata AQI per Kota per Jam ===")
tren_jam_df.show()

# ==========================================
# ANALISIS 3: Ranking Kota Terburuk
# ==========================================
query_ranking = """
    SELECT city, ROUND(AVG(aqi), 2) as rata_rata_aqi, 
    SUM(CASE WHEN aqi > 100 THEN 1 ELSE 0 END) as jumlah_event_tidak_sehat
    FROM air_quality 
    GROUP BY city 
    ORDER BY rata_rata_aqi DESC
"""
ranking_kota_df = spark.sql(query_ranking)

# Menambahkan nomor urut (ranking) menggunakan Window function
windowSpec = Window.orderBy(col("rata_rata_aqi").desc())
ranking_kota_df = ranking_kota_df.withColumn("ranking", row_number().over(windowSpec))

print("\n=== Analisis 3: Ranking Kota dengan AQI Terburuk ===")
ranking_kota_df.show()

# ==========================================
# TAMBAHAN: DATA UNTUK KARTU DASHBOARD (SUMMARY)
# ==========================================
total_records = df_api.count()
avg_aqi_all = df_api.select(spark_round(avg("aqi"), 2)).first()[0]

if ranking_kota_df.count() > 0:
    kota_terburuk = ranking_kota_df.first()["city"]
    total_cities = df_api.select("city").distinct().count()
    
    raw_pct = (ranking_kota_df.filter(col("jumlah_event_tidak_sehat") > 0).count() / total_cities) * 100
    tidak_sehat_pct = float(f"{raw_pct:.2f}")
else:
    kota_terburuk = "-"
    tidak_sehat_pct = 0.0

# ==========================================
# MENYIMPAN HASIL UNTUK DASHBOARD DENGAN KUNCI MILIK RIZKI
# ==========================================
print("\n💾 Menyimpan hasil analisis ke folder Dashboard...")
hasil_spark = {
    "summary": {
        "total_records": total_records,
        "kota_terburuk": kota_terburuk,
        "avg_aqi_all_cities": avg_aqi_all,
        "pct_tidak_sehat": tidak_sehat_pct,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    },
    "analisis_1_distribusi": distribusi_df.toPandas().to_dict(orient="records"),
    "analisis_2_per_jam": tren_jam_df.toPandas().to_dict(orient="records"),
    "analisis_3_ranking": ranking_kota_df.toPandas().to_dict(orient="records")
}

dashboard_dir = "dashboard/data"
os.makedirs(dashboard_dir, exist_ok=True)
json_path = os.path.join(dashboard_dir, "spark_results.json")
with open(json_path, 'w') as f:
    json.dump(hasil_spark, f, indent=4)

print("Done! owarimashitaシ")