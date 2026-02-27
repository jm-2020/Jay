from pyspark.sql.functions import col, current_date, datediff

df = spark.read.format("parquet") \
    .load("gs://jaybucket/bronze/trans/")

df_archive = df.filter(datediff(current_date(), col("ingestion_date")) > 30)

df_active = df.filter(datediff(current_date(), col("ingestion_date")) <= 30)

df_archive.write \
    .mode("append") \
    .parquet("gs://jaybucket/archive/trans/")