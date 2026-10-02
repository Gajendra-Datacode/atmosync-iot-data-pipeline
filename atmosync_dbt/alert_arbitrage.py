import json
import os
import requests
import snowflake.connector

# --- Snowflake Configuration ---
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "GAJENDRA")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "Chavada@123456")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "TOAIPBQ-JM53109")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "ATMOSYNC")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "DEV")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")


def fetch_arbitrage_alerts():
  print("[*] Connecting to Snowflake DEV schema...")
  conn = snowflake.connector.connect(
      user=SNOWFLAKE_USER,
      password=SNOWFLAKE_PASSWORD,
      account=SNOWFLAKE_ACCOUNT,
      warehouse=SNOWFLAKE_WAREHOUSE,
      database=SNOWFLAKE_DATABASE,
      schema=SNOWFLAKE_SCHEMA,
  )
  cursor = conn.cursor()

  query = """
    SELECT 
        CONTAINER_ID,
        COMMODITY_TYPE,
        ORIGINAL_DESTINATION,
        REROUTE_DESTINATION,
        ARBITRAGE_ACTION,
        ESTIMATED_REMAINING_SHELF_LIFE_HRS,
        ROUND((REROUTE_MARKET_PRICE_PER_KG - ORIG_MARKET_PRICE_PER_KG) * 1000, 2) AS POTENTIAL_VALUE_SAVED_USD
    FROM DEV.FCT_SPOILAGE_ARBITRAGE
    WHERE ARBITRAGE_ACTION != 'MAINTAIN_ROUTE'
    ORDER BY ESTIMATED_REMAINING_SHELF_LIFE_HRS ASC;
    """

  cursor.execute(query)
  rows = cursor.fetchall()
  cursor.close()
  conn.close()
  return rows


def dispatch_alert(records):
  if not records:
    print(
        "[OK] No critical arbitrage alerts detected. All cargo is within safe"
        " parameters."
    )
    return

  print(
      f"[!] Found {len(records)} containers requiring spoilage arbitrage"
      " actions.\n"
  )
  alert_lines = []
  for r in records:
    cid, comm, orig, dest, status, risk, saved = r
    saved_val = saved if saved is not None else 0.0
    line = (
        f"- Container {cid} ({comm}) | Action: {status} | Route: {orig} ->"
        f" {dest} | Margin Delta: ${saved_val:,.2f}"
    )
    alert_lines.append(line)

  payload = {
      "title": "AtmoSync Spoilage Arbitrage Alert",
      "total_critical": len(records),
      "alerts": alert_lines,
  }

  if SLACK_WEBHOOK_URL:
    resp = requests.post(SLACK_WEBHOOK_URL, json=payload)
    print(f"[OK] Webhook Response Status: {resp.status_code}")
  else:
    print("--- [ALERTED SPOILAGE ARBITRAGE PAYLOAD] ---")
    print(json.dumps(payload, indent=2))
    print("------------------------------------------")


if __name__ == "__main__":
  try:
    alerts = fetch_arbitrage_alerts()
    dispatch_alert(alerts)
  except Exception as e:
    print(f"[X] Error: {e}")