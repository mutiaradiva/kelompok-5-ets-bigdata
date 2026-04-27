import os
import time
import json
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer

API_TOKEN = os.getenv('AQICN_API_TOKEN', '93e8ac90f39412077ccde8c0f925529344a80d82')
BOOTSTRAP_SERVERS = [server.strip() for server in os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',') if server.strip()]
POLL_INTERVAL_SECONDS = int(os.getenv('POLL_INTERVAL_SECONDS', '900'))

CITIES = {
    'Surabaya': '-7.2504;112.7688',
    'Malang': '-7.9797;112.6304',
    'Sidoarjo': '-7.4478;112.7183',
    'Gresik': '-7.1558;112.6465',
    'Mojokerto': '-7.4664;112.4338'
}

BASE_URL = 'https://api.waqi.info/feed/geo:{}/?token={}'

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8'),
    acks='all',
    enable_idempotence=True
)

TOPIC_NAME = 'airquality-api'

def fetch_air_quality(city_name, coordinates):
    try:
        url = BASE_URL.format(coordinates, API_TOKEN)
        response = requests.get(url)
        data = response.json()
        
        if data['status'] == 'ok':
            aqi_data = data['data']
            payload = {
                'city': city_name.lower(),
                'aqi': aqi_data.get('aqi'),
                'dominentpol': aqi_data.get('dominentpol'),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            return payload
        else:
            print(f"Error API untuk {city_name}: {data.get('data')}")
            return None
    except Exception as e:
        print(f"Gagal mengambil data {city_name}: {e}")
        return None

if __name__ == "__main__":
    print("Mulai menjalankan Producer API (AQICN via GPS Coordinates)...")
    try:
        while True:
            print(f"\n--- Mengambil data pada {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
            
            for city_name, coords in CITIES.items():
                data = fetch_air_quality(city_name, coords)
                if data:
                    producer.send(TOPIC_NAME, key=city_name.lower(), value=data)
                    print(f"Terkirim ke Kafka: {city_name.lower()} -> AQI: {data['aqi']}")
            
            producer.flush() 
            
            print(f"Menunggu {POLL_INTERVAL_SECONDS // 60} menit untuk siklus berikutnya...")
            time.sleep(POLL_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        print("\nProducer dihentikan secara manual.")
    finally:
        producer.close()