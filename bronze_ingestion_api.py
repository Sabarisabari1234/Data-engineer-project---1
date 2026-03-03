import requests
import json
import os
import time
import logging
from datetime import datetime, timezone

# -------- API CONFIG --------
API_URL = "https://fakestoreapi.com/carts"   # Orders API

BASE_PATH = "data-lake/bronze/ecommerce_orders"
LOG_PATH = "logs"

MAX_RETRIES = 3
BACKOFF_SECONDS = 5

# ---------------- LOGGING SETUP ----------------
os.makedirs(LOG_PATH, exist_ok=True)

logging.basicConfig(
    filename=f"{LOG_PATH}/ecommerce_ingestion.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ---------------- INGESTION ----------------
def ingest_orders_data():
    retries = 0

    while retries <= MAX_RETRIES:
        try:
            response = requests.get(API_URL, timeout=30)

            # ---- RATE LIMIT HANDLING ----
            if response.status_code == 429:
                wait_time = BACKOFF_SECONDS * (retries + 1)
                logging.warning(f"Rate limited (429). Waiting {wait_time}s before retry.")
                time.sleep(wait_time)
                retries += 1
                continue

            response.raise_for_status()
            data = response.json()

            if not data:
                logging.info("No order data received from API.")
                print("No order data received.")
                return

            ingestion_ts = datetime.now(timezone.utc).isoformat()

            payload = {
                "ingestion_ts": ingestion_ts,
                "records": data,
            }

            date_part = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            time_part = datetime.now(timezone.utc).strftime("%H_%M_%S")

            output_dir = f"{BASE_PATH}/ingestion_date={date_part}"
            os.makedirs(output_dir, exist_ok=True)

            file_path = f"{output_dir}/orders_{time_part}.json"

            with open(file_path, "w") as f:
                json.dump(payload, f, indent=2)

            logging.info(f"Ingested {len(data)} orders → {file_path}")
            print(f"Ingested {len(data)} orders → {file_path}")
            return

        except requests.exceptions.RequestException as e:
            retries += 1
            logging.error(f"Attempt {retries} failed: {str(e)}")

            if retries > MAX_RETRIES:
                logging.critical("Max retries exceeded. Ingestion failed.")
                raise

            time.sleep(BACKOFF_SECONDS * retries)

# ---------------- ENTRY POINT ----------------
if __name__ == "__main__":
    ingest_orders_data()
