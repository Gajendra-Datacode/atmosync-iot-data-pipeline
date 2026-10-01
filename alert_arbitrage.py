import os
import sys
import json
import requests
import snowflake.connector

SLACK_WEBHOOK_URL=""

def fetch_arbitrage_alerts():
    print("[*] Connecting to Snowflake DEV schema...")
    conn = snowflake.connector.connect(
        user='GAJENDRA',
        password='Chavada@123456',
        account='TOAIPBQ-JM53109',
        warehouse='COMPUTE_WH',
        database='ATMOSYNC',
        schema='DEV'
    )
    cursor = conn.cursor()

    query = """
    SELECT 
        CONTAINER_ID,
        COMMODITY,
        CURRENT_PORT,
        RECOMMENDED_PORT,
        ARBITRAGE_STATUS,
        SPOILAGE_RISK_HOURS,
        POTENTIAL_VALUE_SAVED_USD
    FROM DEV.FCT_SPOILAGE_ARBITRAGE
    WHERE ARBITRAGE_STATUS IN ('CRITICAL_SPOILAGE_IMMPENI', 'REROUTE_RECOMMENDED', 'CRITICAL_SPOILAGE_IMMINENT')
    ORDER BY SPOILAGE_RISK_HOURS ASC;
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

def dispatch_alert(records):
    if not records:
        print("[OK] None of the containers require immediate rerouting.")
        return
    
    print(f"[1] Found {len(records)} containers require spoilage arbitrage actions.\n")
    alert_lines = []
    for r in records:
        cid, comm, orig, dest, status, risk, saved = r
        line = f- Container {cid} ({comm}) | Status: {status} | Divert: {orig} -> {dest} | Margin Saved: ${saved:,.2f}"
        alert_lines.append(line)

    payload = {
        "title": "AtmoSync Spoilage Arbitrage Alert",
        "total_critical": len(records),
        "alerts": alert_lines
    }

    if SLACK_WEBHOOK_URL:
        resp = requests.post(SLACK_WEBHOOK_URL, json=payload)
        print("[OK] Pushed to Slack Webhook!")
    else:
        print("--- [ALLERDTED SPOILAGE ARBITRAGE PAYLOAD] ---")
        print(json.dumps(payload, indent=2))
        print("-----------------------------------------------------")

if __name__ == '__main__':
    try:
        alerts = fetch_arbitrage_alerts()
        dispatch_alert(alerts)
    except Exception as e:
        print(f"[X] Error: {e}")