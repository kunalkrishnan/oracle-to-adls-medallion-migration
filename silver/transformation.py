from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, current_timestamp
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("SilverTransformation").getOrCreate()

# Load bronze tables
sales_df = spark.read.format("delta").load("/mnt/adls/bronze/sales")
customers_df = spark.read.format("delta").load("/mnt/adls/bronze/customers")
products_df = spark.read.format("delta").load("/mnt/adls/bronze/products")

# -----------------------------
# Data Quality Filtering
# -----------------------------

sales_clean = sales_df.filter(
    (col("amount") > 0) &
    (col("customer_id").isNotNull()) &
    (col("product_id").isNotNull())
)

# -----------------------------
# Duplicate Detection
# -----------------------------

windowSpec = Window.partitionBy("order_id").orderBy(col("update_timestamp").desc())

deduplicated_sales = sales_clean.withColumn(
    "row_num",
    row_number().over(windowSpec)
).filter(col("row_num") == 1).drop("row_num")

# -----------------------------
# Join with Dimension Tables
# -----------------------------

sales_joined = deduplicated_sales \
    .join(customers_df, "customer_id", "left") \
    .join(products_df, "product_id", "left")

# -----------------------------
# Business Transformations
# -----------------------------

final_df = sales_joined.select(
    col("order_id"),
    col("customer_id"),
    col("customer_name"),
    col("product_id"),
    col("product_name"),
    col("amount"),
    col("order_date"),
    current_timestamp().alias("etl_load_time")
)

# -----------------------------
# Upsert Logic (MERGE)
# -----------------------------

silver_path = "/mnt/adls/silver/sales_enriched"

if DeltaTable.isDeltaTable(spark, silver_path):

    delta_table = DeltaTable.forPath(spark, silver_path)

    delta_table.alias("target").merge(
        final_df.alias("source"),
        "target.order_id = source.order_id"
    ).whenMatchedUpdate(set={
        "amount": "source.amount",
        "product_id": "source.product_id",
        "etl_load_time": "source.etl_load_time"
    }).whenNotMatchedInsertAll().execute()

else:
    final_df.write.format("delta").mode("overwrite").save(silver_path)
