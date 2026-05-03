# [Ananda Fitri Wibowo]: Setup Spark & Read Local Data
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/default-java"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, hour, to_timestamp, avg, count, round
import json
import pandas as pd

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
            ELSE 'Berbahaya'
        END as kategori_aqi
    FROM air_quality
),
CountData AS (SELECT city, kategori_aqi, COUNT(*) as jumlah FROM KategoriData GROUP BY city, kategori_aqi),
TotalData AS (SELECT city, COUNT(*) as total FROM air_quality GROUP BY city)
SELECT c.city, c.kategori_aqi, c.jumlah, ROUND((c.jumlah / t.total) * 100, 2) as persentase
FROM CountData c JOIN TotalData t ON c.city = t.city ORDER BY c.city, persentase DESC
"""
distribusi_df = spark.sql(query_distribusi)
print("\n=== Analisis 1: Distribusi Kategori AQI ===")
distribusi_df.show()

print("Interpretasi:")
print("- Analisis ini menunjukkan profil kesehatan udara di setiap kota secara proporsional.")
print("- Jika persentase kategori 'Baik' mendominasi, maka kota tersebut layak dipromosikan untuk wisata/hunian sehat.")
print("- Sebaliknya, jika kategori 'Tidak Sehat' tinggi, pemerintah daerah perlu segera mengeluarkan kebijakan darurat.")

# ==========================================
# ANALISIS 2: Rata-rata AQI per Jam
# ==========================================
query_tren_jam = """
    SELECT city, HOUR(CAST(timestamp AS TIMESTAMP)) as jam, ROUND(AVG(aqi), 2) as rata_rata_aqi
    FROM air_quality GROUP BY city, HOUR(CAST(timestamp AS TIMESTAMP)) ORDER BY city, jam ASC
"""
tren_jam_df = spark.sql(query_tren_jam)
print("\n=== Analisis 2: Rata-rata AQI per Kota per Jam ===")
tren_jam_df.show()

print("Interpretasi:")
print("- Analisis tren jam membantu mengidentifikasi pola aktivitas penyebab polusi (seperti jam berangkat kantor atau aktivitas pabrik).")
print("- Data ini krusial bagi warga untuk menentukan jam aman berolahraga di luar ruangan.")
print("- Bagi manajemen kota, ini menjadi dasar untuk mengatur jadwal operasional transportasi publik atau pembatasan kendaraan.")

# ==========================================
# ANALISIS 3: Ranking Kota Terburuk
# ==========================================
query_ranking = """
    SELECT city as kota, ROUND(AVG(aqi), 2) as rata_rata_keseluruhan,
    SUM(CASE WHEN aqi > 100 THEN 1 ELSE 0 END) as total_event_tidak_sehat
    FROM air_quality GROUP BY city ORDER BY rata_rata_keseluruhan DESC
"""
ranking_kota_df = spark.sql(query_ranking)
print("\n=== Analisis 3: Ranking Kota dengan AQI Terburuk ===")
ranking_kota_df.show()

print("Interpretasi:")
print("- Analisis ini memberikan 'Sense of Urgency' bagi pengambil kebijakan di tingkat Provinsi Jawa Timur.")
print("- Kota di peringkat atas (AQI tertinggi) harus menjadi prioritas utama dalam alokasi anggaran perbaikan lingkungan.")
print("- Metrik 'total_kejadian_tidak_sehat' menunjukkan seberapa sering polusi ekstrem terjadi, bukan sekadar rata-rata.")

# ==========================================
# MENYIMPAN HASIL UNTUK DASHBOARD
# ==========================================
print("\n💾 Menyimpan hasil analisis ke folder Dashboard...")
hasil_spark = {
    "distribusi_kategori": distribusi_df.toPandas().to_dict(orient="records"),
    "tren_jam": tren_jam_df.toPandas().to_dict(orient="records"),
    "ranking_kota": ranking_kota_df.toPandas().to_dict(orient="records")
}

dashboard_dir = "dashboard/data"
os.makedirs(dashboard_dir, exist_ok=True)
json_path = os.path.join(dashboard_dir, "spark_results.json")
with open(json_path, 'w') as f:
    json.dump(hasil_spark, f, indent=4)

print("Done! owarimashitaシ")