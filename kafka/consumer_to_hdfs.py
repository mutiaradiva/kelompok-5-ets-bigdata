# [Moch. Rizki Nasrullah - 5027241038]

import os
import json
import time
import threading
import subprocess
import tempfile
from datetime import datetime
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = [server.strip() for server in os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',') if server.strip()]
NAMENODE_CONTAINER = os.getenv('NAMENODE_CONTAINER', 'hadoop-namenode')

HDFS_API_DIR = "/data/airquality/api"
HDFS_RSS_DIR = "/data/airquality/rss"

# Mendapatkan base directory dari file saat ini untuk menemukan folder dashboard
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL_DASHBOARD_API = os.path.join(BASE_DIR, "dashboard", "data", "live_api.json")
LOCAL_DASHBOARD_RSS = os.path.join(BASE_DIR, "dashboard", "data", "live_rss.json")

FLUSH_INTERVAL_SECONDS = 120 # 2 Menit untuk menyimpan buffer ke HDFS

def ensure_hdfs_dir(path):
    """Create HDFS directory via docker exec"""
    cmd = ["docker", "exec", NAMENODE_CONTAINER, "hdfs", "dfs", "-mkdir", "-p", path]
    try:
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:
        print(f"Warning: Could not create HDFS dir {path}: {e}")

def save_to_hdfs(local_file, hdfs_dir):
    """Save file to HDFS via: docker cp (host->container) lalu hdfs dfs -put (container->HDFS).
    
    Metode ini diperlukan karena docker exec tidak bisa mengakses path file dari host Windows.
    """
    filename = os.path.basename(local_file)
    hdfs_path = f"{hdfs_dir}/{filename}"
    container_tmp_path = f"/tmp/{filename}"

    try:
        # Step 1: Copy file dari host ke dalam container
        cp_result = subprocess.run(
            ["docker", "cp", local_file, f"{NAMENODE_CONTAINER}:{container_tmp_path}"],
            capture_output=True, text=True
        )
        if cp_result.returncode != 0:
            print(f"Gagal menyimpan ke HDFS (docker cp): {cp_result.stderr.strip()}")
            return

        # Step 2: Upload dari container ke HDFS
        put_result = subprocess.run(
            ["docker", "exec", NAMENODE_CONTAINER, "hdfs", "dfs", "-put", "-f", container_tmp_path, hdfs_path],
            capture_output=True, text=True
        )
        if put_result.returncode == 0:
            print(f"Berhasil menyimpan ke HDFS: {hdfs_path}")
        else:
            print(f"Gagal menyimpan ke HDFS (hdfs put): {put_result.stderr.strip()}")

        # Step 3: Hapus file sementara di dalam container
        subprocess.run(
            ["docker", "exec", NAMENODE_CONTAINER, "rm", "-f", container_tmp_path],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
    except Exception as e:
        print(f"Error saving to HDFS: {e}")

def update_dashboard_data(filepath, new_data):
    # Pastikan direktori ada
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    current_data = []
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r') as f:
                current_data = json.load(f)
        except Exception:
            pass
            
    current_data.extend(new_data)
    
    # Simpan hanya 50 event terakhir untuk dashboard real-time
    current_data = current_data[-50:]
    
    with open(filepath, 'w') as f:
        json.dump(current_data, f, indent=4)

def consume_topic(topic, hdfs_dir, dashboard_file):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=f"hdfs_consumer_{topic}_group",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    print(f"Mulai membaca dari topik: {topic}")
    
    buffer = []
    last_flush_time = time.time()
    
    ensure_hdfs_dir(hdfs_dir)
    
    try:
        while True:
            # Menggunakan poll agar tidak blocking terus-menerus dan bisa mengecek interval flush
            records = consumer.poll(timeout_ms=1000)
            
            for tp, messages in records.items():
                for message in messages:
                    buffer.append(message.value)
            
            current_time = time.time()
            if current_time - last_flush_time >= FLUSH_INTERVAL_SECONDS and len(buffer) > 0:
                print(f"[{topic}] Mencatat {len(buffer)} event ke HDFS & Dashboard...")
                
                timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                # Gunakan tempfile untuk cross-platform compatibility (Windows & Linux)
                temp_dir = tempfile.gettempdir()
                local_file = os.path.join(temp_dir, f"{topic}_{timestamp_str}.json")
                
                # Buat temporary file (sebagai JSON array yang akan dibaca Spark SQL option multiLine=True)
                with open(local_file, 'w') as f:
                    json.dump(buffer, f, indent=2)
                
                update_dashboard_data(dashboard_file, buffer)
                save_to_hdfs(local_file, hdfs_dir)
                
                # Hapus temporary file lokal
                if os.path.exists(local_file):
                    os.remove(local_file)
                
                buffer.clear()
                last_flush_time = current_time
                
    except KeyboardInterrupt:
        print(f"Berhenti membaca dari {topic}")
    finally:
        consumer.close()

if __name__ == "__main__":
    print("Menjalankan Consumer to HDFS & Dashboard...")
    
    # Menjalankan 2 thread secara paralel untuk topik API dan RSS
    t_api = threading.Thread(target=consume_topic, args=('airquality-api', HDFS_API_DIR, LOCAL_DASHBOARD_API))
    t_rss = threading.Thread(target=consume_topic, args=('airquality-rss', HDFS_RSS_DIR, LOCAL_DASHBOARD_RSS))
    
    t_api.start()
    t_rss.start()
    
    t_api.join()
    t_rss.join()
