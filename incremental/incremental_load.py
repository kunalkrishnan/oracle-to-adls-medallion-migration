from pyspark.sql.functions import col

existing = spark.read.format("delta").load("/mnt/adls/silver/sales_clean")

new_data = spark.read.format("delta").load("/mnt/adls/bronze/sales")

incremental_df = new_data.join(
existing,
"order_id",
"leftanti"
)

incremental_df.write.format("delta") \
.mode("append") \
.save("/mnt/adls/silver/sales_clean")
