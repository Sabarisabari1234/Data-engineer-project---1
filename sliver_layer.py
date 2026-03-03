import os
import json
import pandas as pd
import mysql.connector

BRONZE_PATH = "data-lake/bronze/ecommerce_orders"

# --------------------
# MySQL connection
# --------------------
conn = mysql.connector.connect(
    host="localhost",
    user="admin",
    password="root1",
    database="ecommerce"
)
cursor = conn.cursor()

# --------------------
# Silver ingestion log (INCREMENTAL CONTROL)
# --------------------
cursor.execute("""
CREATE TABLE IF NOT EXISTS silver_ingestion_log (
    file_name VARCHAR(255) PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
conn.commit()

orders = []
order_items = []

# --------------------
# Extract + Transform (INCREMENTAL)
# --------------------
for root, _, files in os.walk(BRONZE_PATH):
    for file in files:
        if not file.endswith(".json"):
            continue

        # ---- Skip already processed files ----
        cursor.execute(
            "SELECT 1 FROM silver_ingestion_log WHERE file_name = %s",
            (file,)
        )
        if cursor.fetchone():
            continue

        file_path = os.path.join(root, file)

        with open(file_path) as f:
            records = json.load(f)["records"]

        for order in records:
            orders.append({
                "order_id": int(order["id"]),
                "user_id": int(order["userId"]),
                "order_date": order["date"]
            })

            for item in order["products"]:
                if item["quantity"] > 0:
                    order_items.append({
                        "order_id": int(order["id"]),
                        "product_id": int(item["productId"]),
                        "quantity": int(item["quantity"]),
                        # ---- MOCK PRICE (Silver enrichment) ----
                        "unit_price": 100.0
                    })

        # ---- Mark file as processed ----
        cursor.execute(
            "INSERT INTO silver_ingestion_log (file_name) VALUES (%s)",
            (file,)
        )
        conn.commit()

# --------------------
# Create DataFrames
# --------------------
orders_df = (
    pd.DataFrame(orders)
    .drop_duplicates(subset=["order_id"])
)

orders_df["order_date"] = pd.to_datetime(
    orders_df["order_date"]
).dt.date

items_df = pd.DataFrame(order_items).drop_duplicates()

# --------------------
# Load into MySQL
# --------------------
orders_data = [
    (row.order_id, row.user_id, row.order_date)
    for row in orders_df.itertuples(index=False)
]

items_data = [
    (row.order_id, row.product_id, row.quantity, row.unit_price)
    for row in items_df.itertuples(index=False)
]

cursor.executemany("""
    INSERT IGNORE INTO silver_orders (order_id, user_id, order_date)
    VALUES (%s, %s, %s)
""", orders_data)

cursor.executemany("""
    INSERT IGNORE INTO silver_order_items
    (order_id, product_id, quantity, unit_price)
    VALUES (%s, %s, %s, %s)
""", items_data)

conn.commit()
cursor.close()
conn.close()

print("✅ Silver layer loaded incrementally into MySQL successfully")
