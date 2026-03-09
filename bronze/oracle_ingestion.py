from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("OracleToBronze").getOrCreate()

oracle_df = spark.read.format("jdbc") \
.option("url","jdbc:oracle:thin:@hostname:1521/orcl") \
.option("dbtable","SALES") \
.option("user","oracle_user") \
.option("password","oracle_password") \
.load()

oracle_df.write.format("delta") \
.mode("append") \
.save("/mnt/adls/bronze/sales")
