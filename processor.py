# processor.py
import os
import json
import logging
from dotenv import load_dotenv
from confluent_kafka import Consumer, Producer, KafkaError

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_RAW = "weather.raw"
TOPIC_PROCESSED = "weather.processed"
TOPIC_FAILED = "weather.failed"


def create_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "weather-processor-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })


def create_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": "weather-processor",
        "enable.idempotence": True,
    })


def enrich(data: dict) -> dict:
    """Ham veriyi zenginleştirir."""
    temp = data["temp_c"]
    humidity = data["humidity"]
    wind = data["wind_kmh"]

    if temp >= 40:
        alert = "🔴 AŞIRI YÜKSEK SICAKLIK"
    elif temp >= 35:
        alert = "🟠 YÜKSEK SICAKLIK"
    elif temp <= 0:
        alert = "🔵 DONMA TEHLİKESİ"
    elif temp <= 5:
        alert = "🟡 DÜŞÜK SICAKLIK"
    else:
        alert = "🟢 NORMAL"

    if temp > 27 and humidity > 40:
        heat_index = round(
            -8.78469475556
            + 1.61139411 * temp
            + 2.33854883889 * humidity
            - 0.14611605 * temp * humidity
            - 0.012308094 * temp ** 2
            - 0.0164248277778 * humidity ** 2
            + 0.002211732 * temp ** 2 * humidity
            + 0.00072546 * temp * humidity ** 2
            - 0.000003582 * temp ** 2 * humidity ** 2,
            1,
        )
    else:
        heat_index = temp

    if wind < 1:
        wind_category = "Durgun"
    elif wind < 20:
        wind_category = "Hafif esinti"
    elif wind < 40:
        wind_category = "Orta rüzgar"
    elif wind < 62:
        wind_category = "Kuvvetli rüzgar"
    else:
        wind_category = "Fırtına"

    return {
        **data,
        "alert": alert,
        "heat_index": heat_index,
        "wind_category": wind_category,
    }


def main():
    consumer = create_consumer()
    producer = create_producer()
    consumer.subscribe([TOPIC_RAW])

    logging.info("🔄 Processor başladı, mesajlar bekleniyor...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logging.error(f"❌ Kafka consumer hatası: {msg.error()}")
                continue

            try:
                raw = json.loads(msg.value().decode("utf-8"))
                processed = enrich(raw)

                producer.produce(
                    topic=TOPIC_PROCESSED,
                    key=msg.key(),
                    value=json.dumps(processed, ensure_ascii=False).encode(),
                )
                producer.poll(0)

                consumer.commit(msg)
                logging.info(
                    f"✅ İşlendi: {raw.get('city')} | "
                    f"{raw.get('temp_c')}°C | {processed.get('alert')}"
                )

            except (json.JSONDecodeError, KeyError) as e:
                logging.error(f"❌ Mesaj işleme hatası: {e}")
                producer.produce(
                    topic=TOPIC_FAILED,
                    key=msg.key(),
                    value=msg.value(),
                )
                producer.poll(0)
                consumer.commit(msg)

    except KeyboardInterrupt:
        logging.info("🛑 Processor durduruluyor...")
    finally:
        consumer.close()
        producer.flush()
        logging.info("✅ Kapatıldı.")


if __name__ == "__main__":
    main()
