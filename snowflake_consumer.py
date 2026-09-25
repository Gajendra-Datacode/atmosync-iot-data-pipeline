import os
import json
from dotenv import load_dotenv
from kafka import KafkaConsumer
import snowflake.connector

# Load variables from .env file
load_dotenv()

# Read Snowflake credentials from environment
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")

# Connect to Snowflake
conn = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT,
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA
)

cursor = conn.cursor()
print("Connected to Snowflake successfully!")

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    "atmosync-telemetry",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="snowflake-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Kafka -> Snowflake consumer started...")

insert_query = """
INSERT INTO ATMOSYNC.RAW.IOT_TELEMETRY (
    CONTAINER_ID,
    TEMPERATURE,
    HUMIDITY,
    VIBRATION,
    EVENT_TIMESTAMP
) VALUES (%s, %s, %s, %s, %s)
"""

try:
    for message in consumer:
        data = message.value
        
        # Extract payload values
        container_id = data.get("container_id")
        temperature = data.get("temperature")
        humidity = data.get("humidity")
        vibration = data.get("vibration")
        event_timestamp = data.get("timestamp")

        # Execute insertion
        cursor.execute(
            insert_query,
            (container_id, temperature, humidity, vibration, event_timestamp)
        )
        conn.commit()

        print(f"Loaded record for {container_id} (Temp: {temperature}°C) into Snowflake")

except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    cursor.close()
    conn.close()
    consumer.close()