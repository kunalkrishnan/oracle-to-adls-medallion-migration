CREATE EXTERNAL TABLE sales_gold_ext
WITH (
LOCATION = 'gold/sales',
DATA_SOURCE = adls_source,
FILE_FORMAT = delta_format
)
AS SELECT * FROM gold_sales;
