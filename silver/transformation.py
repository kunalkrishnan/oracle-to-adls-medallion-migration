from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rank
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

bronze_df = spark.read.format("delta").load("/mnt/adls/bronze/sales")

customer_df = spark.read.format("delta").load("/mnt/adls/bronze/customers")

joined_df = bronze_df.join(
customer_df,
bronze_df.customer_id == customer_df.customer_id,
"inner"
)

windowSpec = Window.partitionBy("customer_id").orderBy(col("order_date").desc())

ranked_df = joined_df.withColumn(
"order_rank",
rank().over(windowSpec)
)

ranked_df.write.format("delta") \
.mode("overwrite") \
.save("/mnt/adls/silver/sales_clean")
