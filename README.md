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
Jalankan Kafka & Hadoop:
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
1. Pastikan *library* Python yang dibutuhkan sudah terinstal:
   ```
   pip install kafka-python requests
   ```
2. Jalankan *script* producer *API*:
   ```
   python kafka/producer_api.py
   ```
  <img width="519" height="175" alt="image" src="https://github.com/user-attachments/assets/e70cbc13-fa56-40e3-ac14-58690c1ab46a" />
