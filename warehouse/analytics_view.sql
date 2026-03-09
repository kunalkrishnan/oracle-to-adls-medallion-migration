CREATE VIEW vw_top_customers AS
SELECT
customer_id,
customer_name,
total_sales,
sales_rank
FROM ext_gold_customer_sales
WHERE sales_rank <= 10;
