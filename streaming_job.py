import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    sum,
    count,
    when,
    date_format
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)


# Environment setup
# Required only for local Windows Spark execution 

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

hadoop_bin = r"C:\hadoop\bin"
if hadoop_bin not in os.environ["PATH"]:
    os.environ["PATH"] = hadoop_bin + ";" + os.environ["PATH"]


# Spark Session

spark = SparkSession.builder \
    .appName("OrderEventsStreamingPipeline") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    ) \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Spark session started")


# Schema

order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("event_time", StringType(), True)
])

print("Schema created")


# Read Kafka Stream

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "order_events") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

print("Kafka stream connected")


# Parse + Watermark + Deduplication

dedup_df = df.selectExpr(
    "CAST(value AS STRING) as json_data"
).select(
    from_json(col("json_data"), order_schema).alias("order_data")
).select(
    col("order_data.order_id").alias("order_id"),
    col("order_data.customer_id").alias("customer_id"),
    col("order_data.product_id").alias("product_id"),
    col("order_data.event_type").alias("event_type"),
    col("order_data.quantity").alias("quantity"),
    col("order_data.price").alias("price"),
    to_timestamp(col("order_data.event_time")).alias("event_timestamp")
).withWatermark(
    "event_timestamp", "10 minutes"
).dropDuplicates([
    "order_id",
    "product_id",
    "event_type",
    "event_timestamp"
])

print("Deduplicated event-time dataframe ready")


# Aggregation 1: Total order value

order_value_window_df = dedup_df.groupBy(
    window(col("event_timestamp"), "5 minutes"),
    col("customer_id")
).agg(
    sum(col("quantity") * col("price")).alias("total_order_value")
)


# Aggregation 2: Cancelled order count

cancelled_orders_window_df = dedup_df.groupBy(
    window(col("event_timestamp"), "5 minutes")
).agg(
    count(
        when(col("event_type") == "CANCELLED", 1)
    ).alias("cancelled_orders_count")
)

print("Aggregations created")


# Add partition column

order_value_output_df = order_value_window_df.select(
    col("window"),
    col("customer_id"),
    col("total_order_value"),
    date_format(col("window.start"), "yyyy-MM-dd").alias("event_date")
)

cancelled_output_df = cancelled_orders_window_df.select(
    col("window"),
    col("cancelled_orders_count"),
    date_format(col("window.start"), "yyyy-MM-dd").alias("event_date")
)

print("Partition-ready dataframes created")


# Write Stream 1

order_value_query = order_value_output_df.writeStream \
    .format("parquet") \
    .option("path", "output/customer_order_value") \
    .option(
        "checkpointLocation",
        "checkpoints/customer_order_value_checkpoint"
    ) \
    .partitionBy("event_date") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

print("Customer order value stream started")


# Write Stream 2

cancelled_query = cancelled_output_df.writeStream \
    .format("parquet") \
    .option("path", "output/cancelled_orders") \
    .option(
        "checkpointLocation",
        "checkpoints/cancelled_orders_checkpoint"
    ) \
    .partitionBy("event_date") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

print("Cancelled orders stream started")


# Keep app running

spark.streams.awaitAnyTermination()