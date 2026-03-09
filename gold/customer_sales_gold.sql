CREATE OR REPLACE TABLE gold_customer_sales
USING DELTA
AS

SELECT
customer_id,
customer_name,
SUM(amount) AS total_sales,
COUNT(order_id) AS total_orders,
RANK() OVER (ORDER BY SUM(amount) DESC) AS sales_rank
FROM delta.`/mnt/adls/silver/sales_enriched`
GROUP BY customer_id, customer_name;
