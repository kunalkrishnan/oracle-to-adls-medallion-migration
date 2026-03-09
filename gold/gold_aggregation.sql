SELECT
customer_id,
customer_name,
SUM(amount) total_sales,
RANK() OVER (ORDER BY SUM(amount) DESC) AS sales_rank
FROM sales_enriched
GROUP BY customer_id, customer_name;
