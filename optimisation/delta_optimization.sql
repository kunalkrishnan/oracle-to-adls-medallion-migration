-- Optimize small files

OPTIMIZE gold_customer_sales;

optimization/delta_optimization.sql

-- Improve filtering performance

OPTIMIZE gold_customer_sales
ZORDER BY (customer_id);

-- Remove old Delta files

VACUUM gold_customer_sales
RETAIN 168 HOURS;
