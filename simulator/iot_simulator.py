import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


# ==============================
# AtmoSync Kafka Configuration
# ==============================

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "atmosync-telemetry"


# ==============================
# Create Kafka Producer
# ==============================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# ==============================
# Simulated Shipping Containers
# ==============================

containers = [
    "CONT_001",
    "CONT_002",
    "CONT_003",
    "CONT_004",
    "CONT_005",
    "CONT_006",
    "CONT_007",
    "CONT_008",
    "CONT_009",
    "CONT_010"
]


print("=" * 70)
print("              ATMOSYNC IoT SIMULATOR")
print("=" * 70)
print(f"Kafka Server : {KAFKA_SERVER}")
print(f"Kafka Topic  : {KAFKA_TOPIC}")
print("Containers   : 10")
print()
print("Sending telemetry...")
print("Press CTRL + C to stop.")
print("=" * 70)


# ==============================
# Generate and Send Telemetry
# ==============================

try:

    while True:

        for container_id in containers:

            telemetry = {
                "container_id": container_id,

                "temperature": round(
                    random.uniform(18.0, 35.0), 2
                ),

                "humidity": round(
                    random.uniform(40.0, 90.0), 2
                ),

                "vibration": round(
                    random.uniform(0.1, 5.0), 2
                ),

                "timestamp": datetime.now(
                    timezone.utc
                ).isoformat()
            }


            # Send telemetry to Kafka
            producer.send(
                KAFKA_TOPIC,
                value=telemetry
            )


            # Display telemetry
            print(
                f"{container_id} | "
                f"Temp: {telemetry['temperature']}°C | "
                f"Humidity: {telemetry['humidity']}% | "
                f"Vibration: {telemetry['vibration']} | "
                f"Time: {telemetry['timestamp']}"
            )


        # Make sure messages are sent
        producer.flush()

        print("-" * 110)

        # Wait 5 seconds
        time.sleep(5)


except KeyboardInterrupt:

    print("\n")
    print("Stopping AtmoSync IoT Simulator...")


finally:

    producer.close()

    print("Kafka producer closed.")
    print("Simulator stopped.")