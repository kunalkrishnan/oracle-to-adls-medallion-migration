MERGE INTO gold_sales AS target
USING new_sales AS source
ON target.customer_id = source.customer_id

WHEN MATCHED THEN
UPDATE SET
target.total_sales = source.total_sales

WHEN NOT MATCHED THEN
INSERT (customer_id,total_sales,total_orders)
VALUES (source.customer_id,source.total_sales,source.total_orders);
