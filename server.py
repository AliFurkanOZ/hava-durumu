# server.py
import os
import json
import asyncio
import threading
import logging
import uuid
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from confluent_kafka import Consumer, KafkaError

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_PROCESSED = "weather.processed"

connected_clients: set[WebSocket] = set()
msg_queue: asyncio.Queue = asyncio.Queue()
latest_data: dict[str, dict] = {}  # Son verileri bellekte tut


def kafka_consumer_thread(loop: asyncio.AbstractEventLoop):
    """Kafka'yı ayrı bir thread'de dinler."""
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": f"ws-broadcast-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC_PROCESSED])
    logging.info("📡 Kafka consumer thread başladı.")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logging.error(f"❌ Kafka hatası: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode("utf-8"))
                city = data.get("city", "unknown")
                latest_data[city] = data  # Bellekte sakla
                asyncio.run_coroutine_threadsafe(msg_queue.put(data), loop)
            except json.JSONDecodeError as e:
                logging.error(f"❌ JSON parse hatası: {e}")

    except Exception as e:
        logging.error(f"❌ Consumer thread hatası: {e}")
    finally:
        consumer.close()


async def broadcast_loop():
    """Queue'dan mesaj alır, tüm istemcilere gönderir."""
    while True:
        data = await msg_queue.get()

        if not connected_clients:
            continue

        results = await asyncio.gather(
            *[safe_send(client, data) for client in list(connected_clients)],
            return_exceptions=True,
        )

        dead_clients = {
            client
            for client, result in zip(list(connected_clients), results)
            if isinstance(result, Exception) or result is False
        }
        connected_clients.difference_update(dead_clients)

        if dead_clients:
            logging.info(f"🔌 {len(dead_clients)} kopuk bağlantı temizlendi.")


async def safe_send(ws: WebSocket, data: dict) -> bool:
    try:
        await ws.send_json(data)
        return True
    except Exception:
        return False


@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_event_loop()

    thread = threading.Thread(
        target=kafka_consumer_thread,
        args=(loop,),
        daemon=True,
        name="kafka-consumer",
    )
    thread.start()

    broadcast_task = asyncio.create_task(broadcast_loop())

    logging.info("🌐 WebSocket sunucusu hazır → ws://localhost:8000/ws")

    yield

    broadcast_task.cancel()
    try:
        await broadcast_task
    except asyncio.CancelledError:
        pass
    logging.info("🛑 Sunucu kapatıldı.")


app = FastAPI(title="Canlı Hava Durumu", lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def serve_index():
    """index.html dosyasını servis eder."""
    index_path = os.path.join(os.path.dirname(__file__), "index.html")
    with open(index_path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    connected_clients.add(ws)
    client_ip = ws.client.host if ws.client else "bilinmiyor"
    logging.info(f"🔗 Yeni bağlantı: {client_ip} | Toplam: {len(connected_clients)}")

    # Yeni bağlanan client'a mevcut verileri hemen gönder
    for city_data in latest_data.values():
        try:
            await ws.send_json(city_data)
        except Exception:
            break

    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        logging.info(f"🔌 Bağlantı koptu: {client_ip}")
    finally:
        connected_clients.discard(ws)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "connected_clients": len(connected_clients),
        "queue_size": msg_queue.qsize(),
        "cities_cached": len(latest_data),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
