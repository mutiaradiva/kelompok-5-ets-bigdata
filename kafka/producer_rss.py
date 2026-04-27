# [Moch. Rizki Nasrullah - 5027241038]

import os
import time
import json
import hashlib
from datetime import datetime, timezone
import feedparser
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

BOOTSTRAP_SERVERS = [
    server.strip()
    for server in os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
    if server.strip()
]
POLL_INTERVAL_SECONDS = 300  # 5 menit

RSS_URL = 'https://news.google.com/rss/search?q=polusi+udara+jawa+timur&hl=id&gl=ID&ceid=ID:id'

TOPIC_NAME = 'airquality-rss'

# Set untuk menyimpan link berita yang sudah dikirim agar tidak duplikat
sent_urls = set()


def create_producer(retries=10, delay=10):
    """Buat KafkaProducer dengan retry agar tahan jika broker belum ready."""
    for attempt in range(1, retries + 1):
        try:
            print(f"[Kafka] Mencoba connect ke {BOOTSTRAP_SERVERS} (percobaan {attempt}/{retries})...")
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8'),
                acks='all',
                enable_idempotence=True,
                request_timeout_ms=30000,
                metadata_max_age_ms=10000,
            )
            print(f"[Kafka] Berhasil connect ke broker!")
            return producer
        except (NoBrokersAvailable, KafkaError) as e:
            print(f"[Kafka] Gagal connect: {e}")
            if attempt < retries:
                print(f"[Kafka] Menunggu {delay} detik sebelum retry...")
                time.sleep(delay)
            else:
                raise RuntimeError(f"Tidak bisa connect ke Kafka setelah {retries} percobaan.") from e


def fetch_and_send_rss(producer, url):
    print(f"Mengambil RSS feed dari: {url}")
    try:
        feed = feedparser.parse(url)

        if feed.bozo and not feed.entries:
            print(f"  [!] Gagal parse RSS dari {url}: {feed.bozo_exception}")
            return

        sent_count = 0
        for entry in feed.entries:
            link = entry.get('link', '')

            if not link or link in sent_urls:
                continue

            title = entry.get('title', '')
            summary = entry.get('summary', '')
            published = entry.get('published', '')

            # Buat hash dari URL untuk dijadikan key (8 karakter hash)
            url_hash = hashlib.md5(link.encode('utf-8')).hexdigest()[:8]

            payload = {
                'title': title,
                'link': link,
                'summary': summary,
                'published': published,
                'source': url,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }

            producer.send(TOPIC_NAME, key=url_hash, value=payload)
            sent_urls.add(link)
            sent_count += 1
            print(f"  Terkirim: {title[:60]}... (key: {url_hash})")

        if sent_count == 0:
            print(f"  Tidak ada berita baru dari {url}")
        else:
            print(f"  Total {sent_count} berita baru dikirim ke Kafka.")

    except Exception as e:
        print(f"  [ERROR] Gagal memproses RSS dari {url}: {e}")


if __name__ == "__main__":
    print("Mulai menjalankan Producer RSS (Tempo & Kompas Polusi/Lingkungan)...")
    print(f"Kafka Bootstrap Servers: {BOOTSTRAP_SERVERS}")

    # Inisialisasi producer dengan retry
    producer = create_producer(retries=10, delay=10)

    try:
        while True:
            print(f"\n--- Memeriksa RSS Feed pada {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

            fetch_and_send_rss(producer, RSS_URL)
            producer.flush(timeout=30)

            print(f"Menunggu {POLL_INTERVAL_SECONDS // 60} menit untuk siklus berikutnya...")
            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nProducer RSS dihentikan secara manual.")
    except Exception as e:
        print(f"\n[FATAL] Error tidak terduga: {e}")
    finally:
        producer.close()
