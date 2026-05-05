# Ananda Fitri Wibowo — 5027241057

import os
import time
os.environ["JAVA_HOME"] = "/usr/lib/jvm/default-java"

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, avg, round as spark_round, row_number, date_format
import json
import pandas as pd
from datetime import datetime

print("⏳ Memulai mesin Spark (Continuous Streaming Mode)...")
spark = SparkSession.builder \
    .appName("AirQualityAnalysis") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .config("spark.sql.session.timeZone", "Asia/Jakarta") \
    .getOrCreate()

path_hdfs_input = "hdfs://localhost:8020/data/airquality/api"
path_hdfs_output = "hdfs://localhost:8020/data/airquality/results"

while True:
    waktu_sekarang = datetime.now().strftime('%H:%M:%S')
    print(f"\n[{waktu_sekarang}] 📥 Membaca data terbaru dari HDFS...")
    
    try:
        df_api = spark.read.option("multiLine", True).json(path_hdfs_input)
        df_api = df_api.dropDuplicates(["city", "timestamp"])
        
        # PERBAIKAN ZONA WAKTU: Biarkan Spark melakukan cast native ke Asia/Jakarta
        df_api = df_api.withColumn("timestamp_wib", col("timestamp").cast("timestamp"))
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

        # ==========================================
        # ANALISIS 2: Rata-rata per Jam (Format HH:00 untuk 24 Jam)
        # ==========================================
        query_tren_jam = """
            SELECT 
                city, 
                DATE_FORMAT(timestamp_wib, 'HH:00') as hour, 
                ROUND(AVG(aqi), 2) as avg_aqi
            FROM air_quality 
            WHERE DATE(timestamp_wib) = CURRENT_DATE()
            GROUP BY city, hour 
            ORDER BY city, hour ASC
        """
        tren_jam_df = spark.sql(query_tren_jam)

        # ==========================================
        # ANALISIS 3: Ranking Kota
        # ==========================================
        query_ranking = """
            SELECT city, ROUND(AVG(aqi), 2) as rata_rata_aqi, 
            SUM(CASE WHEN aqi > 100 THEN 1 ELSE 0 END) as jumlah_event_tidak_sehat
            FROM air_quality 
            GROUP BY city 
            ORDER BY rata_rata_aqi DESC
        """
        ranking_kota_df = spark.sql(query_ranking)
        windowSpec = Window.orderBy(col("rata_rata_aqi").desc())
        ranking_kota_df = ranking_kota_df.withColumn("ranking", row_number().over(windowSpec))

        # ==========================================
        # SIMPAN KE HDFS
        # ==========================================
        distribusi_df.write.mode("overwrite").json(f"{path_hdfs_output}/distribusi")
        tren_jam_df.write.mode("overwrite").json(f"{path_hdfs_output}/tren_jam")
        ranking_kota_df.write.mode("overwrite").json(f"{path_hdfs_output}/ranking")

        # ==========================================
        # UPDATE LOCAL JSON
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
        with open(os.path.join(dashboard_dir, "spark_results.json"), 'w') as f:
            json.dump(hasil_spark, f, indent=4)

        print("✅ Analisis batch sukses. Update berikutnya dalam 60 detik...")
        
    except Exception as e:
        print(f"⚠️ Menunggu data masuk: {e}")

    # Jeda 60 detik untuk tiap batch
    time.sleep(60)