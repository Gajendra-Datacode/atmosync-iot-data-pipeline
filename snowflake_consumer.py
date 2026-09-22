import os
import json
from dotenv import load_dotenv
import snowflake.connector
from kafka import KafkaConsumer

load_dotenv()

KAFKA_TOPIC = "atmosync-telemetry"
KAFKA_SERVER = "localhost:9092"

# Connect to Kafka
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# Connect to Snowflake
conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

cursor = conn.cursor()

print("Kafka → Snowflake consumer started...")
print("Waiting for IoT telemetry...")

try:
    for message in consumer:
        data = message.value

        cursor.execute(
            """
            INSERT INTO IOT_TELEMETRY
            (CONTAINER_ID, TEMPERATURE, HUMIDITY, VIBRATION, TIMESTAMP)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                data.get("container_id"),
                data.get("temperature"),
                data.get("humidity"),
                data.get("vibration"),
                data.get("timestamp")
            )
        )

        conn.commit()

        print(
            f"Saved to Snowflake: "
            f"{data.get('container_id')} | "
            f"Temp: {data.get('temperature')}°C | "
            f"Humidity: {data.get('humidity')}% | "
            f"Vibration: {data.get('vibration')}"
        )

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    cursor.close()
    conn.close()
    consumer.close()