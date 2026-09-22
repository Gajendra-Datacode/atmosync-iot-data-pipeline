import json
import csv
import os
from kafka import KafkaConsumer

# Kafka configuration
KAFKA_TOPIC = "atmosync-telemetry"
KAFKA_SERVER = "localhost:9092"

# CSV output path
CSV_FILE = os.path.join("data", "iot_telemetry.csv")

# Create data folder if it doesn't exist
os.makedirs("data", exist_ok=True)

# Create Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# Create CSV file with headers if it doesn't exist
file_exists = os.path.exists(CSV_FILE)

with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)

    if not file_exists:
        writer.writerow([
            "container_id",
            "temperature",
            "humidity",
            "vibration",
            "timestamp"
        ])

    print("Kafka → CSV consumer started...")
    print(f"Saving data to: {CSV_FILE}")

    for message in consumer:
        data = message.value

        writer.writerow([
            data.get("container_id"),
            data.get("temperature"),
            data.get("humidity"),
            data.get("vibration"),
            data.get("timestamp")
        ])

        file.flush()

        print(
            f"Saved: {data.get('container_id')} | "
            f"Temp: {data.get('temperature')}°C | "
            f"Humidity: {data.get('humidity')}% | "
            f"Vibration: {data.get('vibration')}"
        )