import mysql.connector

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
# Create Gold DB & Tables
# --------------------
cursor.execute("""
CREATE DATABASE IF NOT EXISTS ecommerce_gold
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS ecommerce_gold.gold_watermark (
    pipeline_name VARCHAR(50) PRIMARY KEY,
    last_processed_date DATE
)
""")

cursor.execute("""
INSERT IGNORE INTO ecommerce_gold.gold_watermark
VALUES ('gold_daily_sales', '1900-01-01')
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS ecommerce_gold.gold_daily_sales (
    order_date DATE PRIMARY KEY,
    total_orders INT,
    total_items_sold INT,
    total_revenue DECIMAL(10,2),
    avg_order_value DECIMAL(10,2)
)
""")

conn.commit()

# --------------------
# Incremental Gold Load
# --------------------
cursor.execute("""
INSERT INTO ecommerce_gold.gold_daily_sales
SELECT
    DATE(o.order_date) AS order_date,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.quantity) AS total_items_sold,
    SUM(oi.quantity * oi.unit_price) AS total_revenue,
    ROUND(
        SUM(oi.quantity * oi.unit_price) / COUNT(DISTINCT o.order_id),
        2
    ) AS avg_order_value
FROM ecommerce.silver_orders o
JOIN ecommerce.silver_order_items oi
    ON o.order_id = oi.order_id
WHERE o.order_date >
    (
        SELECT last_processed_date
        FROM ecommerce_gold.gold_watermark
        WHERE pipeline_name = 'gold_daily_sales'
    )
GROUP BY DATE(o.order_date)
ON DUPLICATE KEY UPDATE
    total_orders = VALUES(total_orders),
    total_items_sold = VALUES(total_items_sold),
    total_revenue = VALUES(total_revenue),
    avg_order_value = VALUES(avg_order_value)
""")

conn.commit()

# --------------------
# Update watermark
# --------------------
cursor.execute("""
UPDATE ecommerce_gold.gold_watermark
SET last_processed_date = (
    SELECT MAX(order_date)
    FROM ecommerce.silver_orders
)
WHERE pipeline_name = 'gold_daily_sales'
""")

conn.commit()
cursor.close()
conn.close()

print("✅ Gold layer loaded incrementally and successfully")
