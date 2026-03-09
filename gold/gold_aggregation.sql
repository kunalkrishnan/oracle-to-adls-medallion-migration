CREATE TABLE gold_sales
USING DELTA
AS

SELECT
customer_id,
SUM(amount) total_sales,
COUNT(order_id) total_orders
FROM delta.`/mnt/adls/silver/sales_clean`
GROUP BY customer_id;
