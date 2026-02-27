from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IngestCSVParquetToDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df_csv = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/jay/csv/")

df_parquet = spark.read.format("parquet") \
    .load("/jay/parquet/")

common_cols = list(set(df_csv.columns).intersection(set(df_parquet.columns)))

df_csv_aligned = df_csv.select(common_cols)
df_parquet_aligned = df_parquet.select(common_cols)

df_combined = df_csv_aligned.unionByName(df_parquet_aligned)

df_combined.write.format("delta") \
    .mode("append") \
    .save("/delta/input_files")