# ETS-Big-Data-Kelompok-5

## **Anggota Kelompok:**
- Raynard Carlent — 5027241109
- Naufal Ardhana — 5027241118
- Ananda Fitri Wibowo — 5027241057
- Mutiara Diva Jaladitha — 5027241083
- Moch Rizki Nasrullah — 5027241038

### Topik 3 — AirQuality Alert: Indeks Kualitas Udara Jawa Timur

**Skenario klien:** Dinas Kesehatan Provinsi Jawa Timur yang ingin memantau AQI kota-kota besar dan mengirimkan peringatan saat kualitas udara memburuk.

**Pertanyaan bisnis yang harus dijawab:**
> *"Pada jam berapa kualitas udara paling buruk, dan apakah berita lingkungan mencerminkan kondisi tersebut?"*

| | Detail |
|-|--------|
| **API Real-time** | AQICN World Air Quality Index API — **gratis setelah daftar**, API key instan |
| **Endpoint hint** | `https://api.waqi.info/feed/[city]/?token=[your_key]` — ganti `[city]` dengan `surabaya`, `malang`, `sidoarjo`, dst. |
| **Alternatif tanpa key** | OpenAQ API: `https://api.openaq.org/v2/latest?city=Surabaya&limit=10` — gratis tanpa key |
| **Kota yang dipantau** | Surabaya, Malang, Sidoarjo, Gresik, Mojokerto (5 kota Gerbangkertasusila) |
| **Interval polling** | Setiap 15 menit |
| **RSS Feed** | `https://rss.tempo.co/tag/polusi` |
| **Backup RSS** | `https://rss.kompas.com/feed/kompas.com/sains/environment` |
| **Kafka Topic 1** | `airquality-api` — key: nama kota |
| **Kafka Topic 2** | `airquality-rss` — key: hash URL |
| **HDFS Path** | `/data/airquality/api/` dan `/data/airquality/rss/` |

**3 Analisis Spark Wajib:**
1. **Distribusi kategori AQI:** klasifikasikan setiap event — Baik (0–50), Sedang (51–100), Tidak Sehat (101–150), Berbahaya (>150) — hitung persentase per kota
2. **Rata-rata AQI per kota per jam:** identifikasi jam puncak polusi menggunakan Spark SQL
3. **Kota dengan AQI terburuk:** ranking kota dari rata-rata AQI tertinggi ke terendah, sertakan jumlah event "Tidak Sehat" atau lebih buruk

**Fokus dashboard:** Tabel AQI per kota dengan indikator warna (hijau/kuning/oranye/merah) · Kategorisasi kondisi · Berita lingkungan

---

### Cara Menjalankan
Sebelum menjalankan Docker Compose, buat file `.env` di root project dan isi token AQICN:
```
AQICN_API_TOKEN=token_kamu_di_sini
```

Jalankan Kafka, producer API, & Hadoop:
```
docker compose -f docker-compose-kafka.yml up -d
docker compose -f docker-compose-hadoop.yml up -d
```

Masuk ke container
```
docker exec -it kafka-broker bash
```

Membuat topik
```
/opt/kafka/bin/kafka-topics.sh --create --topic airquality-api --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
/opt/kafka/bin/kafka-topics.sh --create --topic airquality-rss --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```
<img width="1546" height="153" alt="Screenshot 2026-04-27 194828" src="https://github.com/user-attachments/assets/2f6aaa0b-f6ba-4bc1-a040-4eaf604bed72" />

---

### Integrasi API Eksternal
**File Utama:** `kafka/producer_api.py`

Bagian ini bertanggung jawab untuk melakukan *ingestion* data kualitas udara secara *real-time* dari API eksternal ke dalam sistem Kafka.

**Fitur yang Diimplementasikan:**
* **Sumber Data:** Terintegrasi dengan **AQICN API** untuk memantau 5 kota di wilayah Gerbangkertasusila (Surabaya, Malang, Sidoarjo, Gresik, Mojokerto).
* **Bypass Koordinat:** Menggunakan *mapping* titik koordinat GPS (`geo:lat;lng`) untuk pencarian data guna mengatasi masalah "Unknown station" pada API bawaan.
* **Keandalan Data (Kafka):** Konfigurasi producer menggunakan `acks='all'` dan `enable_idempotence=True` untuk menjamin tidak ada data yang hilang atau duplikat (Exactly-Once Semantics/At-Least-Once yang andal).
* **Automasi & Standarisasi:** *Polling* berjalan terus-menerus dengan interval **15 menit**, dan data distandarisasi ke format JSON lengkap dengan `timestamp` (ISO 8601).

**Cara Menjalankan:**
1. Jalankan service Kafka dan producer API lewat Docker Compose:
   ```
   docker compose -f docker-compose-kafka.yml up -d --build
   ```
   Compose akan otomatis membaca `AQICN_API_TOKEN` dari file `.env` di root project.
2. Jika ingin menjalankan script langsung di host, pasang dependensi Python terlebih dahulu:
   ```
   pip install kafka-python requests
   ```
3. Jalankan *script* producer *API* secara manual hanya jika tidak memakai container:
   ```
   python kafka/producer_api.py
   ```
  <img width="519" height="175" alt="image" src="https://github.com/user-attachments/assets/e70cbc13-fa56-40e3-ac14-58690c1ab46a" />

---

### Integrasi RSS Feed (Producer RSS)
**File Utama:** `kafka/producer_rss.py`

Bagian ini melakukan *ingestion* berita kualitas udara dari RSS feed secara berkala ke topik Kafka `airquality-rss`.

**Fitur yang Diimplementasikan:**
* **Sumber Data:** Google News RSS (query `polusi udara jawa timur`) sebagai sumber utama, dengan Kompas Sains/Environment sebagai sumber backup.
* **Deduplication:** Menyimpan set URL berita yang sudah dikirim sehingga berita yang sama tidak dikirim dua kali dalam satu sesi.
* **Keandalan Koneksi:** Menggunakan retry logic dengan 10 percobaan dan jeda 10 detik per percobaan agar tahan jika Kafka broker belum siap.
* **Interval Polling:** Berjalan terus-menerus dengan interval **5 menit**.

**Cara Menjalankan:**

1. Pastikan Kafka sudah berjalan (lihat bagian *Cara Menjalankan* di atas).
2. Install dependensi Python:
   ```bash
   pip install kafka-python feedparser
   ```
3. Jalankan producer RSS dari root project:
   ```bash
   python kafka/producer_rss.py
   ```
4. Output sukses yang diharapkan:
   ```
   [Kafka] Mencoba connect ke ['localhost:9092'] (percobaan 1/10)...
   [Kafka] Berhasil connect ke broker!
   --- Memeriksa RSS Feed pada 2026-04-28 01:00:00 ---
   Mengambil RSS feed dari: https://news.google.com/rss/...
     Terkirim: Kualitas Udara Surabaya Memburuk... (key: a1b2c3d4)
     Total 5 berita baru dikirim ke Kafka.
   Menunggu 5 menit untuk siklus berikutnya...
   ```

---

### Consumer to HDFS
**File Utama:** `kafka/consumer_to_hdfs.py`

Bagian ini membaca pesan dari kedua topik Kafka (`airquality-api` dan `airquality-rss`) secara paralel, lalu menyimpannya ke HDFS dan memperbarui data dashboard lokal.

**Fitur yang Diimplementasikan:**
* **Dual Thread:** Menjalankan dua consumer secara paralel dalam thread terpisah — satu untuk topik API, satu untuk topik RSS.
* **Buffer & Batch Write:** Data diakumulasi dalam buffer dan di-flush ke HDFS setiap **2 menit** dalam format JSON array.
* **Upload ke HDFS (Windows-compatible):** Menggunakan `docker cp` untuk menyalin file dari host ke container, lalu `hdfs dfs -put` dari dalam container — karena `docker exec` tidak bisa mengakses path file Windows secara langsung.
* **Dashboard Update:** Menyimpan 50 event terakhir ke `dashboard/data/live_api.json` dan `dashboard/data/live_rss.json` untuk keperluan visualisasi real-time.

**Cara Menjalankan:**

1. Pastikan Kafka **dan** Hadoop sudah berjalan:
   ```bash
   docker compose -f docker-compose-kafka.yml up -d
   docker compose -f docker-compose-hadoop.yml up -d
   ```
2. Install dependensi Python:
   ```bash
   pip install kafka-python
   ```
3. Jalankan consumer dari folder `kafka/`:
   ```bash
   python kafka/consumer_to_hdfs.py
   ```
4. Output sukses yang diharapkan:
   ```
   Menjalankan Consumer to HDFS & Dashboard...
   Mulai membaca dari topik: airquality-rss
   Mulai membaca dari topik: airquality-api
   [airquality-api] Mencatat 5 event ke HDFS & Dashboard...
   Berhasil menyimpan ke HDFS: /data/airquality/api/airquality-api_2026-04-28_01-00-00.json
   [airquality-rss] Mencatat 12 event ke HDFS & Dashboard...
   Berhasil menyimpan ke HDFS: /data/airquality/rss/airquality-rss_2026-04-28_01-00-00.json
   ```

> **Catatan:** Consumer menggunakan `auto_offset_reset="earliest"` sehingga saat pertama kali dijalankan akan membaca **semua pesan** yang sudah ada di topik sejak awal.

---

### Cara Verifikasi (Testing)

#### 1. Cek Topik Kafka Berisi Data
Masuk ke container Kafka dan lihat pesan yang masuk:
```bash
# Lihat pesan di topik airquality-api
docker exec -it kafka-broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic airquality-api \
  --from-beginning \
  --max-messages 5

# Lihat pesan di topik airquality-rss
docker exec -it kafka-broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic airquality-rss \
  --from-beginning \
  --max-messages 5
```

#### 2. Cek Consumer Group Aktif
```bash
docker exec kafka-broker /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list
```
Output yang diharapkan:
```
hdfs_consumer_airquality-rss_group
hdfs_consumer_airquality-api_group
```

#### 3. Cek File Tersimpan di HDFS
```bash
# Cek direktori
docker exec hadoop-namenode hdfs dfs -ls /data/airquality/

# Cek isi folder API
docker exec hadoop-namenode hdfs dfs -ls /data/airquality/api/

# Cek isi folder RSS
docker exec hadoop-namenode hdfs dfs -ls /data/airquality/rss/
```
Output yang diharapkan:
```
Found 2 items
drwxr-xr-x   - hadoop supergroup  0 ... /data/airquality/api
drwxr-xr-x   - hadoop supergroup  0 ... /data/airquality/rss
```

#### 4. Baca Isi File JSON di HDFS
```bash
# Lihat isi file terakhir yang tersimpan (ganti nama file sesuai output ls)
docker exec hadoop-namenode hdfs dfs -cat /data/airquality/api/airquality-api_2026-04-28_01-00-00.json
```

#### 5. Cek HDFS via Web UI
Buka browser dan akses NameNode Web UI:
```
http://localhost:9870
```
Navigasi ke **Utilities → Browse the file system** → masuk ke path `/data/airquality/`.

<img width="1000" alt="HDFS Web UI" src="https://github.com/user-attachments/assets/placeholder-hdfs-ui" />

---

### Revisi dari Demo Project
##### 1) Spark dapat membaca data dari HDFS
Berhasil terhubung ke port HDFS, mengubah kode pada "analysis.py".
- Sebelum:
```
path_local_input = "data/airquality/api" 
df_api = spark.read.option("multiLine", True).json(path_local_input)
```
- Sesudah:
```

path_hdfs_input = "hdfs://localhost:8020/data/airquality/api"
path_hdfs_output = "hdfs://localhost:8020/data/airquality/results"
```
- Note:
Agar sparknya dapat berjalan, perlu dilakukan 2 hal tambahan:
###### a) Edit file /etc/hosts , tambahkan 127.0.0.1 datanode.
   ```
   nano /etc/hosts
   # tambahkan di paling bawah
   127.0.0.1 datanode
   ```
###### b) Setelah docker nyala, jalankan
```
docker exec -it hadoop-namenode hdfs dfs -chmod -R 777 /data
```
agar spark-nya mendapat izin untuk menyimpan hasil analisis ke HDFS.

##### 2) Hasil analisis disimpan ke HDFS
Hasil data yang telah dianalisis disimpan ke HDFS:
- Path tujuan di dalam ekosistem Hadoop:
```
path_hdfs_output = "hdfs://localhost:8020/data/airquality/results"
```
- Menulis data ke HDFS dengan mode 'overwrite' untuk memperbarui data lama:
```
distribusi_df.write.mode("overwrite").json(f"{path_hdfs_output}/distribusi")
tren_jam_df.write.mode("overwrite").json(f"{path_hdfs_output}/tren_jam")
ranking_kota_df.write.mode("overwrite").json(f"{path_hdfs_output}/ranking")
```
<img width="1419" height="228" alt="Screenshot 2026-05-05 112940" src="https://github.com/user-attachments/assets/fd407ff7-f374-4cc3-bb42-2236cc7d1e18" />
<img width="1436" height="510" alt="Screenshot 2026-05-05 113147" src="https://github.com/user-attachments/assets/556a4d87-a9be-47bc-9420-e3b1a6af0c15" />
<img width="1439" height="286" alt="Screenshot 2026-05-05 113039" src="https://github.com/user-attachments/assets/a9c5b1a8-a75e-4cf1-acea-8ef0aa36659a" />

##### 3) Analisis berjalan secara kontinu
Membuat analisis berjalan secara kontinu dan mengupdate analisis setiap 60 detik
```
print("⏳ Memulai mesin Spark (Continuous Streaming Mode)...")

while True: # Infinite loop agar program tidak pernah mati
    waktu_sekarang = datetime.now().strftime('%H:%M:%S')
    print(f"\n[{waktu_sekarang}] 📥 Membaca data terbaru dari HDFS...")
    
    try:
        # ... (proses baca data HDFS, dropDuplicates, SQL analisis) ...
        # ... (proses simpan data ke HDFS & JSON lokal) ...

        print("✅ Analisis batch sukses. Update berikutnya dalam 60 detik...")
        
    except Exception as e:
        # Fault Tolerance: Kalau ada error (misal data kosong), 
        # mesin tidak mati, melainkan sekadar melapor dan mencoba lagi.
        print(f"⚠️ Menunggu data masuk: {e}")

    # Jeda komputasi selama 60 detik sebelum melakukan analisis batch selanjutnya
    time.sleep(60)
```
<img width="1440" height="314" alt="Screenshot 2026-05-05 113310" src="https://github.com/user-attachments/assets/24ad357c-5808-4e80-82d1-b002a1347c86" />

##### 4) Memperbaiki tampilan Dashboard
Tampilan dashboard pada bagian "Analisis Tren Waktu" diperbarui.
<img width="1919" height="1090" alt="Screenshot 2026-05-05 112507" src="https://github.com/user-attachments/assets/8e55b681-2e6d-496c-a762-0c3d78058311" />
<img width="1919" height="1092" alt="Screenshot 2026-05-05 112527" src="https://github.com/user-attachments/assets/1aa35218-a452-42ad-9db1-1823dfc2d8dc" />
<img width="634" height="358" alt="Screenshot 2026-05-05 103742" src="https://github.com/user-attachments/assets/4fa6bb93-d5d3-40d4-a979-9a20672d7909" />


### Troubleshooting

| Error | Penyebab | Solusi |
|-------|----------|--------|
| `KafkaTimeoutError: Failed to update metadata` | Kafka broker belum ready atau salah alamat | Tunggu beberapa detik lalu coba lagi. Pastikan `localhost:9092` bisa diakses dari host |
| `No such container: namenode` | Nama container salah | Nama yang benar adalah `hadoop-namenode` (sesuai `container_name` di compose) |
| `Gagal menyimpan ke HDFS (docker cp)` | HDFS belum ready atau namenode belum healthy | Cek status: `docker ps` dan pastikan `hadoop-namenode` statusnya `(healthy)` |
| Container Hadoop langsung exit | Format env var salah di `hadoop.env` | Gunakan format `CORE-SITE.XML_key=value` (bukan `CORE_CONF_`) sesuai image `apache/hadoop:3` |
| `ValueError: too many values to unpack` (di log container) | Sama seperti di atas | Lihat baris di atas |
