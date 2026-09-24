import json
import os
from datetime import datetime

from dotenv import load_dotenv
from kafka import KafkaConsumer
import snowflake.connector


# ============================================================
# Load environment variables
# ============================================================

load_dotenv()


# ============================================================
# Configuration
# ============================================================

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "iot-telemetry"

SNOWFLAKE_DATABASE = "ATMOSYNC"
SNOWFLAKE_SCHEMA = "RAW"
SNOWFLAKE_TABLE = "IOT_TELEMETRY"


# ============================================================
# Kafka Consumer
# ============================================================

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,

    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),

    auto_offset_reset="latest",
    enable_auto_commit=False,

    group_id="atmosync-snowflake-consumer"
)


# ============================================================
# Snowflake Connection
# ============================================================

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA
)

cursor = conn.cursor()


print("=" * 75)
print("          ATMOSYNC KAFKA → SNOWFLAKE")
print("=" * 75)

print(f"Kafka Topic : {KAFKA_TOPIC}")
print(
    f"Snowflake   : "
    f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}"
)

print()
print("Waiting for telemetry...")
print("Press CTRL + C to stop.")
print("=" * 75)


# ============================================================
# Snowflake INSERT
# ============================================================

insert_query = f"""
INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
(
    CONTAINER_ID,
    TEMPERATURE,
    HUMIDITY,
    VIBRATION,
    EVENT_TIMESTAMP
)
VALUES (%s, %s, %s, %s, %s)
"""


# ============================================================
# Process Kafka Messages
# ============================================================

try:

    for message in consumer:

        telemetry = message.value

        container_id = telemetry["container_id"]
        temperature = telemetry["temperature"]
        humidity = telemetry["humidity"]
        vibration = telemetry["vibration"]

        event_timestamp = datetime.fromisoformat(
            telemetry["timestamp"].replace("Z", "+00:00")
        )

        # Insert into Snowflake
        cursor.execute(
            insert_query,
            (
                container_id,
                temperature,
                humidity,
                vibration,
                event_timestamp
            )
        )

        conn.commit()

        # Commit Kafka offset only after successful Snowflake insert
        consumer.commit()

        print(
            f"INSERTED | "
            f"{container_id} | "
            f"Temp: {temperature}°C | "
            f"Humidity: {humidity}% | "
            f"Vibration: {vibration}"
        )


except KeyboardInterrupt:

    print("\nStopping consumer...")


except Exception as error:

    print("\nERROR:")
    print(error)


finally:

    cursor.close()
    conn.close()
    consumer.close()

    print("Connections closed.")