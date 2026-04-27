# ETS-Big-Data-Kelompok-5

## **Anggota Kelompok:**
- Raynard Carlent — 5027241109
- Naufal Ardhana — 5027241118
- Ananda Fitri Wibowo — 5027241057
- Mutiara Diva Jaladitha — 5027241083
- Moch Rizki Nasrullah — 5027241038

## Topik
### Topik 3 — 🌫️ AirQuality Alert: Indeks Kualitas Udara Jawa Timur

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