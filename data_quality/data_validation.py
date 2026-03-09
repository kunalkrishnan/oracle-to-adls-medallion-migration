from pyspark.sql.functions import col

def validate_sales(df):

    valid_df = df.filter(
        (col("order_id").isNotNull()) &
        (col("customer_id").isNotNull()) &
        (col("amount") > 0)
    )

    duplicate_check = df.groupBy("order_id").count().filter("count > 1")

    if duplicate_check.count() > 0:
        print("Duplicate records found")

    return valid_df
