CREATE EXTERNAL TABLE ext_gold_customer_sales
(
customer_id INT,
customer_name STRING,
total_sales FLOAT,
total_orders INT,
sales_rank INT
)
WITH
(
LOCATION = 'gold/customer_sales',
DATA_SOURCE = adls_source,
FILE_FORMAT = delta_format
);
