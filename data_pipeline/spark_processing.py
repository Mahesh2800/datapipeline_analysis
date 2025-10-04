from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, count, sum as spark_sum, avg,
    max as spark_max, min as spark_min,
    date_trunc, countDistinct, when, lit, coalesce,
    current_timestamp 
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from psycopg2 import connect as pg_connect
from datetime import datetime
import time

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPICS = "orders,payments,inventory"
DB_USER = "postgres"
DB_PASSWORD = "123"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "ecommerse_db"
DB_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
CHECKPOINT_LOCATION = "file:///C:/data/spark_checkpoints/ecom-pipeline" 

# Schemas must be defined for ALL topics
order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("price", DoubleType(), True)
])

payments_schema = StructType([
    StructField("payment_id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("amount", DoubleType(), True),
])

inventory_schema = StructType([
    StructField("product_name", StringType(), True),
    StructField("stock_change", IntegerType(), True),
    StructField("warehouse", StringType(), True),
])

# PostgreSQL connection properties for Spark JDBC
db_properties = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

TOP_PRODUCTS_VIEW_SQL = """
CREATE OR REPLACE VIEW top_products AS
SELECT 
    product_name,
    total_orders,
    total_sales,
    avg_price
FROM product_analytics
ORDER BY total_sales DESC
LIMIT 10;
"""

## >>> NEW SQL DEFINITION FOR USER ANALYTICS VIEW
TOP_USERS_VIEW_SQL = """
CREATE OR REPLACE VIEW top_users AS
SELECT 
    user_id,
    total_orders,
    total_spent,
    avg_order_value,
    user_segment
FROM user_analytics
ORDER BY total_spent DESC
LIMIT 10;
"""

# --------------------------------------------------------
#                     NEW RAW WRITE FUNCTIONS (UNCHANGED)
# --------------------------------------------------------

def write_payments(df_payments):
    """Writes raw payments data to the payments table."""
    df_payments.select(
        col("payment_id").alias("payment_id"),
        col("order_id").alias("order_id"),
        col("status").alias("status"),
        col("amount").alias("amount"),
        col("kafka_timestamp").alias("created_at") 
    ).write \
        .jdbc(url=DB_URL, table="payments", mode="append", properties=db_properties)
    print(f"--> Saved {df_payments.count()} raw payments to payments")


def write_inventory(df_inventory):
    """Writes raw inventory data to the inventory_updates table."""
    df_inventory.select(
        col("product_name").alias("product_name"),
        col("stock_change").alias("stock_change"),
        col("warehouse").alias("warehouse"),
        col("kafka_timestamp").alias("created_at")
    ).write \
        .jdbc(url=DB_URL, table="inventory_updates", mode="append", properties=db_properties)
    print(f"--> Saved {df_inventory.count()} raw inventory updates to inventory_updates")


# --------------------------------------------------------
#                           UPSERT FUNCTIONS (UNCHANGED)
# --------------------------------------------------------
def upsert_processed_orders(batch_df, batch_id):
    """Handles exactly-once writing for orders_processed (DO NOTHING on conflict)."""
    conn = None
    cursor = None
    try:
        # 1. Spark writes the current batch data to a temporary staging table
        batch_df.select(
            "order_id", "user_id", "product_name", "price", 
            col("order_date").alias("order_date"),
            "price_category", "is_valid"
        ).write \
            .jdbc(url=DB_URL, table="orders_processed_staging", mode="overwrite", properties=db_properties)

        # 2. Execute the SQL MERGE/UPSERT operation
        conn = pg_connect(database=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        conn.autocommit = True
        cursor = conn.cursor()

        upsert_sql = """
        INSERT INTO orders_processed 
        (order_id, user_id, product_name, price, order_date, price_category, is_valid)
        SELECT 
            order_id, user_id, product_name, price, order_date, price_category, is_valid
        FROM orders_processed_staging
        ON CONFLICT (order_id) DO NOTHING;
        """
        cursor.execute(upsert_sql)
        print("--> Saved processed orders with conflict resolution (UPSERT).")
        
    except Exception as e:
        print(f"Error during UPSERT: {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def upsert_hourly_sales(batch_df, batch_id):
    """Handles time-series aggregation using ON CONFLICT DO UPDATE (MERGE)."""
    conn = None
    cursor = None
    try:
        # 1. Spark writes the current batch data to a temporary staging table
        batch_df.select(
            "hour_timestamp", "total_orders", "total_revenue", "unique_users", "avg_order_value"
        ).write \
            .jdbc(url=DB_URL, table="hourly_sales_staging", mode="overwrite", properties=db_properties)

        # 2. Execute the SQL MERGE/UPSERT operation
        conn = pg_connect(database=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        conn.autocommit = True
        cursor = conn.cursor()

        upsert_sql = """
        INSERT INTO hourly_sales AS h
        (hour_timestamp, total_orders, total_revenue, unique_users, avg_order_value)
        SELECT 
            hour_timestamp, total_orders, total_revenue, unique_users, avg_order_value
        FROM hourly_sales_staging
        ON CONFLICT (hour_timestamp) DO UPDATE 
        SET 
            total_orders = h.total_orders + EXCLUDED.total_orders,
            total_revenue = h.total_revenue + EXCLUDED.total_revenue,
            unique_users = h.unique_users + EXCLUDED.unique_users,
            avg_order_value = (h.total_revenue + EXCLUDED.total_revenue) / (h.total_orders + EXCLUDED.total_orders);
        """
        cursor.execute(upsert_sql)
        print("--> Saved time-series hourly_sales with conflict resolution (UPDATE).")
        
    except Exception as e:
        print(f"Error during hourly UPSERT: {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

# --- NEW FUNCTION: Full Recalculation for User Analytics (REQUIRED FOR GENAI)
def upsert_user_analytics(batch_df, batch_id):
    """Calculates cumulative user analytics from the full processed history and manages table overwrite."""
    try:
        # Read the full history from the processed table (where data is de-duplicated)
        full_processed_df = spark.read \
            .jdbc(url=DB_URL, table="orders_processed", properties=db_properties)
    except Exception as e:
        print(f"Error reading orders_processed for user analytics: {e}")
        return

    user_analytics_cumulative = full_processed_df.groupBy("user_id").agg(
        count("order_id").alias("total_orders"),
        spark_sum("price").alias("total_spent"),
        avg("price").alias("avg_order_value"),
        spark_min("order_date").alias("first_order_date"),
        spark_max("order_date").alias("last_order_date")
    )
    
    # Add user segment and the CRITICAL updated_at timestamp
    final_user_analytics = user_analytics_cumulative.withColumn(
        "user_segment",
        when(col("total_spent") > 5000, "vip")
        .when(col("total_spent") > 2000, "regular")
        .otherwise("new")
    ).withColumn(
        "updated_at", current_timestamp() # <<< ADDED THE TIMESTAMP HERE
    ).select(
        "user_id", "total_orders", "total_spent", "avg_order_value",
        "first_order_date", "last_order_date", "user_segment",
        "updated_at" # <<< INCLUDE IN FINAL SELECT
    )
    
    # --- CRITICAL CHANGE: Call the new management function ---
    write_user_analytics_and_refresh_view(final_user_analytics) # Call the new function
    print("--> Updated cumulative user_analytics (Full Recalculation)") 

# --- NEW FUNCTION: View Management for User Analytics
def write_user_analytics_and_refresh_view(df):
    """Writes user_analytics table using overwrite and refreshes the dependent top_users view."""
    conn = None
    cursor = None
    
    try:
        conn = pg_connect(database=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # CRITICAL FIX: Forcefully drop the dependent view
        cursor.execute("DROP VIEW IF EXISTS top_users CASCADE;")
        print("--> Dropped top_users view successfully.")
        
        # 2. Spark writes the DataFrame (mode="overwrite" now works)
        df.write \
            .jdbc(url=DB_URL, table="user_analytics", mode="overwrite", properties=db_properties)
        
        # 3. Recreate the view after the table has been recreated by Spark
        cursor.execute(TOP_USERS_VIEW_SQL)
        print("--> Refreshed top_users view.")

    except Exception as e:
        print(f"Error during USER ANALYTICS write/refresh: {e}")
        raise # Re-raise the exception to fail the batch properly

    finally:
        if cursor: cursor.close()
        if conn: conn.close()


# --------------------------------------------------------
#                    VIEW REFRESH FUNCTION (UNCHANGED)
# --------------------------------------------------------

def write_product_analytics_and_refresh_view(df, db_url, db_props, view_sql):
    """Writes product_analytics table using overwrite and manages the dependent view."""
    conn = None
    cursor = None
    
    try:
        # 1. Connect to the DB and forcefully drop the dependent view
        conn = pg_connect(database=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # CRITICAL FIX: Drop dependent views using CASCADE
        cursor.execute("DROP VIEW IF EXISTS top_products CASCADE;")
        print("--> Dropped dependent views successfully.")
        
        # 2. Spark writes the DataFrame (mode="overwrite" now works)
        df.write \
            .jdbc(url=db_url, table="product_analytics", mode="overwrite", properties=db_properties)
        
        # 3. Recreate the view after the table has been recreated by Spark
        cursor.execute(view_sql)
        print("--> Refreshed top_products view.")

    except Exception as e:
        print(f"Error during PRODUCT ANALYTICS write/refresh: {e}")
        raise 

    finally:
        if cursor: cursor.close()
        if conn: conn.close()

# --- SPARK SESSION --- 
spark = SparkSession.builder \
    .appName("KafkaToPostgresStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("="*60)
print("KAFKA TO POSTGRESQL ANALYTICS PIPELINE - STARTED")
print("="*60)

# --------------------------------------------------------
#                      CORE PROCESSING FUNCTION
# --------------------------------------------------------
def foreach_batch_function(current_batch_df, batch_id):
    print(f"\n--- Processing Batch ID: {batch_id} ---")
    current_batch_df.persist()
    
    try:
        # 1. ROUTE DATA BY TOPIC NAME
        current_batch_df = current_batch_df.withColumn("topic", col("topic"))

        orders_df = current_batch_df.filter(col("topic") == "orders")
        payments_df = current_batch_df.filter(col("topic") == "payments")
        inventory_df = current_batch_df.filter(col("topic") == "inventory")

        # ----------------------------------------------------
        # PROCESS ORDERS TOPIC (The primary data source)
        # ----------------------------------------------------
        if orders_df.count() > 0:
            parsed_orders_df = orders_df.select(
                from_json(col("value").cast("string"), order_schema).alias("data"),
                col("timestamp").alias("kafka_timestamp") 
            ).select("data.*", "kafka_timestamp").withColumn(
                "created_at", col("kafka_timestamp") 
            )
            print(f"Processing {parsed_orders_df.count()} order records.")

            # A. RAW ORDER SAVE
            parsed_orders_df.select(
                "order_id", "user_id", "product_name", "price", "kafka_timestamp", "created_at"
            ).write \
                .jdbc(url=DB_URL, table="orders_raw", mode="append", properties=db_properties)
            print(f"--> Saved {parsed_orders_df.count()} raw orders to orders_raw")

            # B. PROCESSED ORDER & UPSERT
            processed_orders_df = parsed_orders_df.withColumn(
                "price_category",
                when(col("price") < 500, "low").when((col("price") >= 500) & (col("price") < 1000), "medium").otherwise("high")
            ).withColumn("order_date", col("kafka_timestamp")).withColumn("is_valid", col("price") > 0)
            
            upsert_processed_orders(processed_orders_df, batch_id)

            # C. USER ANALYTICS UPDATE (CRITICAL FOR GENAI)
            # This must run AFTER upsert_processed_orders to ensure orders_processed is up-to-date
            # NEW: CALLS THE VIEW-MANAGEMENT FUNCTION
            upsert_user_analytics(processed_orders_df, batch_id)
            
            # D. ANALYTICS (PRODUCT & HOURLY) 
            product_analytics_batch = parsed_orders_df.groupBy("product_name").agg(
                count("order_id").alias("total_orders"), spark_sum("price").alias("total_sales"),
                spark_max("price").alias("max_price"), spark_min("price").alias("min_price"),
                spark_max("kafka_timestamp").alias("last_order_date")
            )
            final_product_analytics = product_analytics_batch.withColumn(
                "avg_price", col("total_sales") / col("total_orders")
            ).select("product_name", "total_orders", "total_sales", "avg_price", "max_price", "min_price", "last_order_date")
            
            write_product_analytics_and_refresh_view(df=final_product_analytics, db_url=DB_URL, db_props=db_properties, view_sql=TOP_PRODUCTS_VIEW_SQL)
            print("--> Updated cumulative product_analytics")
            
            hourly_sales_batch = parsed_orders_df.withColumn(
                "hour_timestamp", date_trunc("hour", col("kafka_timestamp"))
            ).groupBy("hour_timestamp").agg(
                count("order_id").alias("total_orders"), spark_sum("price").alias("total_revenue"),
                countDistinct("user_id").alias("unique_users"), avg("price").alias("avg_order_value")
            )
            upsert_hourly_sales(hourly_sales_batch, batch_id)


        # ----------------------------------------------------
        # PROCESS PAYMENTS TOPIC
        # ----------------------------------------------------
        if payments_df.count() > 0:
            parsed_payments_df = payments_df.select(
                from_json(col("value").cast("string"), payments_schema).alias("data"),
                col("timestamp").alias("kafka_timestamp") 
            ).select("data.*", "kafka_timestamp")
            
            write_payments(parsed_payments_df)


        # ----------------------------------------------------
        # PROCESS INVENTORY TOPIC
        # ----------------------------------------------------
        if inventory_df.count() > 0:
            parsed_inventory_df = inventory_df.select(
                from_json(col("value").cast("string"), inventory_schema).alias("data"),
                col("timestamp").alias("kafka_timestamp") 
            ).select("data.*", "kafka_timestamp")

            write_inventory(parsed_inventory_df)

        
        if orders_df.count() == 0 and payments_df.count() == 0 and inventory_df.count() == 0:
            print("No new records in this batch.")


    except Exception as e:
        print(f"\n--- ERROR in Batch {batch_id} ---")
        print(f"Error details: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        current_batch_df.unpersist()


# --------------------------------------------------------
#                      MAIN STREAM EXECUTION
# --------------------------------------------------------
try:
    # 1. READ FROM ALL KAFKA TOPICS
    kafka_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPICS) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. APPLY FOREACHBATCH WRITER
    query = kafka_stream_df.writeStream \
        .outputMode("append") \
        .trigger(processingTime='5 seconds') \
        .foreachBatch(foreach_batch_function) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .start()

    print("\nStreaming query started. Press Ctrl+C to stop.")
    query.awaitTermination()


except KeyboardInterrupt:
    print("\n\nPipeline stopped by user (Ctrl+C)")
except Exception as e:
    print(f"\nFATAL ERROR: {e}")
finally:
    spark.stop()
    print("\nSpark session closed")
