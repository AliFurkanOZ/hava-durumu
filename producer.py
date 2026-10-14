# producer.py
import os
import time
import json
import logging
import requests
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
API_KEY = os.getenv("OPENWEATHER_API_KEY")
TOPIC_RAW = "weather.raw"
CITIES = ["istanbul", "ankara", "izmir", "bursa", "antalya", "bitlis", "trabzon","giresun"]
INTERVAL_SECONDS = 120  # Free API: dakikada 60 istek sınırı var


def delivery_report(err, msg):
    """Kafka'ya gönderim sonucunu loglar."""
    if err:
        logging.error(f"❌ Gönderim hatası: {err}")
    else:
        logging.info(
            f"✅ Gönderildi → topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def fetch_weather(city: str) -> dict | None:
    """OpenWeatherMap API'den hava verisini çeker."""
    url = (
        f"https://api.openweathermap.org/data/2.5/weather"
        f"?q={city}&appid={API_KEY}&units=metric&lang=tr"
    )
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return {
            "city": city,
            "temp_c": round(data["main"]["temp"], 1),
            "feels_like": round(data["main"]["feels_like"], 1),
            "humidity": data["main"]["humidity"],
            "wind_kmh": round(data["wind"]["speed"] * 3.6, 1),
            "description": data["weather"][0]["description"],
            "icon": data["weather"][0]["icon"],
            "timestamp": int(time.time()),
            "lat": data["coord"]["lat"],
            "lon": data["coord"]["lon"],
        }
    except requests.exceptions.HTTPError as e:
        logging.error(f"❌ {city} HTTP hatası: {e}")
    except requests.exceptions.Timeout:
        logging.error(f"❌ {city} zaman aşımı")
    except Exception as e:
        logging.error(f"❌ {city} beklenmedik hata: {e}")
    return None


def main():
    if not API_KEY or API_KEY in ("buraya_api_anahtarinizi_yazin", "your_api_key_here"):
        logging.error("❌ OPENWEATHER_API_KEY bulunamadı! Lütfen .env dosyasında geçerli bir anahtar tanımlayın.")
        return

    producer = Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": "weather-producer",
        "retries": 5,
        "retry.backoff.ms": 200,
        "enable.idempotence": True,
    })

    logging.info(f"🚀 Producer başladı. Şehirler: {CITIES}")

    try:
        while True:
            for city in CITIES:
                payload = fetch_weather(city)
                if payload:
                    producer.produce(
                        topic=TOPIC_RAW,
                        key=city.encode(),
                        value=json.dumps(payload, ensure_ascii=False).encode(),
                        callback=delivery_report,
                    )
                    producer.poll(0)

            # flush(): Bekleyen tüm mesajları Kafka'ya gönderir
            producer.flush()
            logging.info(f"⏳ {INTERVAL_SECONDS} saniye bekleniyor...")
            time.sleep(INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logging.info("🛑 Producer durduruluyor...")
        producer.flush()
        logging.info("✅ Kapatıldı.")


if __name__ == "__main__":
    main()
