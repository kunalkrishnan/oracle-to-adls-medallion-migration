from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, current_timestamp, broadcast
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ----------------------------------------
# Spark Session Configuration (Optimization)
# ----------------------------------------

spark = SparkSession.builder \
.appName("SilverTransformation") \
.config("spark.sql.shuffle.partitions", "200") \
.config("spark.sql.adaptive.enabled", "true") \
.config("spark.sql.adaptive.skewJoin.enabled", "true") \
.getOrCreate()

# ----------------------------------------
# Load Bronze Tables
# ----------------------------------------

sales_df = spark.read.format("delta").load("/mnt/adls/bronze/sales")
customers_df = spark.read.format("delta").load("/mnt/adls/bronze/customers")
products_df = spark.read.format("delta").load("/mnt/adls/bronze/products")

# ----------------------------------------
# Data Quality Filtering
# ----------------------------------------

sales_clean = sales_df.filter(
    (col("amount") > 0) &
    (col("customer_id").isNotNull()) &
    (col("product_id").isNotNull())
)

# ----------------------------------------
# Duplicate Detection
# ----------------------------------------

windowSpec = Window.partitionBy("order_id").orderBy(col("update_timestamp").desc())

deduplicated_sales = sales_clean.withColumn(
    "row_num",
    row_number().over(windowSpec)
).filter(col("row_num") == 1).drop("row_num")

# ----------------------------------------
# Broadcast Join Optimization
# ----------------------------------------

sales_joined = deduplicated_sales \
.join(broadcast(customers_df), "customer_id", "left") \
.join(broadcast(products_df), "product_id", "left")

# ----------------------------------------
# Cache Data (Used Multiple Times)
# ----------------------------------------

sales_joined.cache()
sales_joined.count()

# ----------------------------------------
# Repartition For Parallel Processing
# ----------------------------------------

sales_joined = sales_joined.repartition(8, "customer_id")

# ----------------------------------------
# Business Transformations
# ----------------------------------------

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

# ----------------------------------------
# Delta Upsert Logic
# ----------------------------------------

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
    final_df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("order_date") \
        .save(silver_path)

# ----------------------------------------
# Clear Cache
# ----------------------------------------

spark.catalog.clearCache()
