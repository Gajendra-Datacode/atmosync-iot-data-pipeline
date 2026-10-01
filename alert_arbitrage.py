import os
import json
import requests
import snowflake.connector

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "GAJENDRA")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "TOAIPBQ-JM53109")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "ATMOSYNC")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "DEV")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")


def fetch_arbitrage_alerts():
    print("[*] Connecting to Snowflake DEV schema...")

    if not SNOWFLAKE_PASSWORD:
        raise ValueError("SNOWFLAKE_PASSWORD environment variable is not set.")

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

    cursor = conn.cursor()

    query = """
    SELECT
        CONTAINER_ID,
        COMMODITY_TYPE,
        ORIGINAL_DESTINATION,
        REROUTE_DESTINATION,
        AVG_TEMP_C,
        MAX_SAFE_TEMP_C,
        ESTIMATED_REMAINING_SHELF_LIFE_HRS,
        TRANSIT_TIME_ORIG_HRS,
        TRANSIT_TIME_REROUTE_HRS,
        ARBITRAGE_ACTION
    FROM ATMOSYNC.DEV.FCT_SPOILAGE_ARBITRAGE
    WHERE ARBITRAGE_ACTION IN (
        'CRITICAL_SPOILAGE_IMMINENT',
        'REROUTE_RECOMMENDED'
    )
    ORDER BY ESTIMATED_REMAINING_SHELF_LIFE_HRS ASC
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return rows


def dispatch_alert(records):

    if not records:
        print(
            "[OK] No critical arbitrage alerts detected. "
            "All cargo is within acceptable thresholds."
        )
        return

    print(
        f"[!] Found {len(records)} containers requiring "
        "reroute / salvage actions."
    )

    alert_lines = []

    for row in records:
        (
            container_id,
            commodity,
            origin,
            reroute,
            avg_temp,
            max_safe_temp,
            remaining_shelf,
            transit_orig,
            transit_reroute,
            action
        ) = row

        line = (
            f"- Container {container_id} ({commodity}) | "
            f"Action: {action} | "
            f"Route: {origin} -> {reroute} | "
            f"Temperature: {avg_temp}°C / Safe: {max_safe_temp}°C | "
            f"Remaining shelf life: {remaining_shelf} hrs | "
            f"Original transit: {transit_orig} hrs | "
            f"Reroute transit: {transit_reroute} hrs"
        )

        alert_lines.append(line)

    payload = {
        "text": "AtmoSync Real-Time Spoilage Arbitrage Alert",
        "summary": "Cargo requiring reroute or urgent action",
        "alerts": alert_lines
    }

    if SLACK_WEBHOOK_URL:
        response = requests.post(
            SLACK_WEBHOOK_URL,
            json=payload,
            timeout=15
        )

        if response.status_code == 200:
            print("[OK] Alert successfully delivered to Slack webhook!")
        else:
            print(
                f"[X] Failed to send Slack alert: "
                f"{response.status_code} - {response.text}"
            )

    else:
        print(
            "\n--- [SIMULATED SLACK/EMAIL NOTIFICATION] ---"
        )
        print(json.dumps(payload, indent=2))
        print("-----------------------------------------------\n")
        print(
            "[INFO] Live Slack webhook is optional. "
            "Alert system successfully executed."
        )


if __name__ == "__main__":
    try:
        critical_items = fetch_arbitrage_alerts()
        dispatch_alert(critical_items)

    except Exception as error:
        print(f"[X] Error during alert check: {error}")
