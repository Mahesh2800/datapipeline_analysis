from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("EcommerceRealTimeProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Define schema for order data
order_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("user_id", IntegerType()),
    StructField("product_name", StringType()),
    StructField("price", DecimalType(10,2))
])

# 3. Read data from Kafka
orders_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .load()

# 4. Convert Kafka value (binary) → JSON → DataFrame
orders_parsed = orders_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), order_schema).alias("data")) \
    .select("data.*")

# 5. Example processing → total sales per product
aggregated = orders_parsed.groupBy("product_name").sum("price")

# 6. Output to console (later we’ll write to PostgreSQL)
query = aggregated.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()