from pyspark.sql.functions import max

existing = spark.read.format("delta").load("/mnt/adls/silver/sales_enriched")

max_date = existing.select(max("order_date")).collect()[0][0]

new_data = spark.read.format("delta").load("/mnt/adls/bronze/sales")

incremental_df = new_data.filter(col("order_date") > max_date)

incremental_df.write.format("delta") \
.mode("append") \
.save("/mnt/adls/silver/sales_enriched")
