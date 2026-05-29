# Data Lakehouse — Kelompok 5
### Medallion Architecture: Bronze · Silver · Gold · Time Travel
**AirQuality Alert — Kualitas Udara Jawa Timur**

| | |
|---|---|
| **Mata Kuliah** | Big Data |
| **Kelompok** | 5 |
| **Topik** | AirQuality Alert — Kualitas Udara Jawa Timur |
| **Anggota** | Raynard Carlent (5027241109) · Naufal Ardhana (5027241118) · Ananda Fitri Wibowo (5027241057) · Mutiara Diva Jaladitha (5027241083) · Moch Rizki Nasrullah (5027241038) |

---

## Arsitektur Pipeline

### Sebelum — ETS
```
[AQICN API]          [RSS Feed]
     |                    |
producer_api.py     producer_rss.py
     |                    |
     +──────[Kafka]───────+
                 |
       consumer_to_hdfs.py
                 |
  HDFS /data/airquality/api/*.json   ← JSON mentah, no ACID
  HDFS /data/airquality/rss/*.json   ← no versioning, no schema
                 |
       spark/analysis.ipynb          ← baca JSON langsung
                 |
       dashboard/app.py
```

### Sesudah — Lakehouse (folder `lakehouse/` ditambahkan, ETS tidak diubah)
```
HDFS /data/airquality/api/*.json   (dari ETS, tidak diubah)
               |
         01_bronze.py
               |
  BRONZE  ./lakehouse_data/bronze/airquality_api/
          Delta Lake — data mentah + _ingested_at + _source + _delta_log/
               |
         02_silver.py
               |
  SILVER  ./lakehouse_data/silver/airquality/
          Deduplikasi · Filter null · Timestamp WIB · Kolom jam
               |
         03_gold.py
               |
  GOLD    ./lakehouse_data/gold/
          ├── aqi_category_dist/   ← Repro ETS Analisis 1
          ├── aqi_hourly/          ← Repro ETS Analisis 2
          └── aqi_trend/           ← Enhanced: Window Function
               |
         04_time_travel.py
               |
  Demonstrasi ACID: update data → rusak → rollback versi lama
```

---

## 1. Bronze Layer — `01_bronze.py`

Ingest data mentah dari HDFS ke Delta Lake tanpa mengubah konten apapun.

**Yang dilakukan:**
- Baca file JSON dari HDFS (`/data/airquality/api/` dan `/rss/`)
- Tambah kolom `_ingested_at` (waktu ingest) dan `_source` (`"api"` atau `"rss"`)
- Simpan ke Delta Lake dengan `mode("append")` — data lama tidak tertimpa
- Buat folder `_delta_log/` yang berisi transaction log Delta Lake

**Output Terminal:**

![Bronze Output](docs/screenshots/ss_bronze.png)

**Struktur Folder & Delta Log:**

![Direktori Lakehouse](docs/screenshots/ss_direktori.png)

![Delta Log](docs/screenshots/ss_delta_log.png)

> Folder `_delta_log/` adalah yang membedakan Bronze dari sekadar file Parquet biasa. Setiap operasi tulis dicatat di sini — dari sinilah Time Travel dan ACID bekerja.

---

## 2. Silver Layer — `02_silver.py`

Cleaning dan normalisasi data dari Bronze.

| # | Transformasi | Kode | Alasan |
|---|---|---|---|
| 1 | Hapus Duplikat | `dropDuplicates(["city", "timestamp"])` | API AQICN kadang kirim data sama 2x |
| 2 | Filter Null AQI | `filter(col("aqi").isNotNull())` | Record tanpa AQI merusak Window Function di Gold |
| 3 | Konversi ke WIB | `from_utc_timestamp(to_timestamp(col("timestamp")), "Asia/Jakarta")` | Timestamp dari API adalah String UTC — dikonversi ke TimestampType WIB |
| 4 | Ekstrak Kolom Jam | `date_format(col("timestamp_wib"), "HH:00")` | Kolom jam siap pakai untuk groupBy di Gold |

**Output Terminal:**

![Silver Output](docs/screenshots/ss_silver.png)

> Jumlah baris di Silver selalu ≤ Bronze. Selisih = jumlah duplikat + record null AQI yang berhasil dibersihkan.

---

## 3. Gold Layer — `03_gold.py`

Tiga tabel agregasi siap pakai.

### Tabel 1 — Distribusi Kategori AQI *(Repro ETS Analisis 1)*

```python
gold_category = silver.withColumn("kategori_aqi",
    when(col("aqi") <= 50, "Baik")
    .when((col("aqi") > 50) & (col("aqi") <= 100), "Sedang")
    .when((col("aqi") > 100) & (col("aqi") <= 150), "Tidak Sehat")
    .when((col("aqi") > 150) & (col("aqi") <= 200), "Sangat Tidak Sehat")
    .otherwise("Berbahaya")
).groupBy("kategori_aqi").count()
```

### Tabel 2 — Rata-rata AQI per Jam *(Repro ETS Analisis 2)*

```python
gold_hourly = silver.groupBy("city", "jam").agg(
    spark_round(avg("aqi"), 2).alias("avg_aqi")
).orderBy("city", "jam")
```

### Tabel 3 — Tren AQI dengan Window Function *(Enhanced — tidak ada di ETS)*

```python
window_spec = Window.partitionBy("city").orderBy("timestamp_wib")

gold_trend = silver \
    .withColumn("prev_aqi", lag("aqi", 1).over(window_spec)) \
    .withColumn("aqi_change", col("aqi") - col("prev_aqi")) \
    .withColumn("trend",
        when(col("aqi_change") > 5,  "Memburuk")
        .when(col("aqi_change") < -5, "Membaik")
        .otherwise("Stabil")
    ) \
    .groupBy("city", "trend").count()
```

> **Mengapa tidak bisa di ETS:** Window Function butuh `TimestampType` untuk `orderBy` yang akurat secara kronologis. Di ETS timestamp masih `String` — `"2025-01-10 10:00" > "2025-01-09 23:00"` secara string benar, tapi secara waktu mereka beda 11 jam, bukan 1 jam.

**Output Terminal:**

![Gold Output](docs/screenshots/ss_gold.png)

---

## 4. Time Travel — `04_time_travel.py`

Demonstrasi ACID Delta Lake: simulasi data rusak dan recovery.

| Step | Aksi | Hasil |
|---|---|---|
| 1 | Tampilkan history awal | Versi 0: WRITE — Delta Lake mencatat operasi |
| 2 | Update semua city → `"KOTA DARURAT"` | Simulasi script yang salah merusak data |
| 3 | Query data sekarang | Hanya ada `KOTA DARURAT` — data corrupt |
| 4 | Time Travel ke versi 0 | Data asli kembali: surabaya, malang, gresik, dll |
| 5 | Tampilkan history akhir | Versi 0: WRITE · Versi 1–3: UPDATE |

**Output Terminal:**

![Time Travel Output](docs/screenshots/ss_time_travel.png)

> Ini tidak mungkin dilakukan dengan HDFS biasa — data yang ditimpa hilang permanen. Delta Lake menyimpan semua versi di `_delta_log/`.

---

## 5. Verifikasi Semua Layer — `intip_data.py`

![Intip Data Output](docs/screenshots/ss_intip_data.png)

---

## 6. Perbandingan ETS vs Lakehouse

| Aspek | ETS (HDFS + JSON) | Lakehouse (Delta Lake) |
|---|---|---|
| Format | JSON mentah di HDFS | Parquet + `_delta_log/` |
| ACID | Tidak ada | Ada — setiap operasi transaksional |
| Time Travel | Tidak bisa | `versionAsOf` — query versi manapun |
| Schema | Tidak ada enforcement | Dipaksa — error jika tidak sesuai |
| Duplikat | Tidak terdeteksi | Dibersihkan di Silver |
| Timestamp | String UTC | TimestampType WIB |
| Window Function | Tidak akurat | Akurat (TimestampType ordering) |
| Tren AQI | Tidak ada | `gold/aqi_trend` dengan `lag()` |
| Recovery data rusak | Tidak bisa | Rollback dengan `versionAsOf` |

---

## 7. Cara Menjalankan

### Prasyarat
```bash
# Install library
py -m pip install pyspark==3.5.0 delta-spark==3.1.0 kafka-python

# Isi file .env
AQICN_API_TOKEN=token_kamu_disini
```

### Urutan Lengkap
```bash
# 1. Nyalakan infrastruktur
docker compose -f docker-compose-kafka.yml up -d
docker compose -f docker-compose-hadoop.yml up -d

# 2. Buat direktori HDFS
docker exec hadoop-namenode hdfs dfs -mkdir -p /data/airquality/api
docker exec hadoop-namenode hdfs dfs -mkdir -p /data/airquality/rss

# 3. Kumpulkan data (tunggu minimal 15 menit)
py kafka/consumer_to_hdfs.py

# 4. Jalankan pipeline Lakehouse (terminal baru)
py lakehouse/01_bronze.py
py lakehouse/02_silver.py
py lakehouse/03_gold.py
py lakehouse/04_time_travel.py

# 5. Verifikasi semua layer
py lakehouse/intip_data.py
```

### Struktur Folder Akhir
```
kelompok-5-ets-bigdata/
├── kafka/                    ← tidak diubah (ETS)
├── dashboard/                ← tidak diubah (ETS)
├── spark/                    ← tidak diubah (ETS)
├── lakehouse/                ← FOLDER BARU
│   ├── 01_bronze.py
│   ├── 02_silver.py
│   ├── 03_gold.py
│   ├── 04_time_travel.py
│   └── intip_data.py
├── lakehouse_data/           ← dibuat otomatis
│   ├── bronze/airquality_api/
│   │   ├── _delta_log/       ← transaction log Delta Lake
│   │   └── part-*.parquet
│   ├── silver/airquality/
│   └── gold/
│       ├── aqi_category_dist/
│       ├── aqi_hourly/
│       └── aqi_trend/
├── docs/screenshots/         ← folder screenshot
├── docker-compose-hadoop.yml
├── docker-compose-kafka.yml
└── .env
```
