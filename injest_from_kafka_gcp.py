from pyspark.sql import SparkSession

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType


spark = SparkSession.builder \
    .appName("KafkaToMybucket") \
    .getOrCreate()

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host:9092") \
    .option("subscribe", "Tran_topic") \
    .option("startingOffsets", "latest") \
    .load()

schema = StructType() \
    .add("tran_id", StringType()) \
    .add("cust_id", StringType()) \
    .add("amt", DoubleType())

df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

query = df_parsed.writeStream \
    .format("parquet") \
    .option("path", "gs://jay-bucket/trans/") \
    .option("checkpointLocation", "gs://jay-bucket/checkpoints/") \
    .outputMode("append") \
    .start()

query.awaitTermination()