CREATE TABLE test_db.monthly_sales (
    month_date Date,
    category LowCardinality(String),
    region LowCardinality(String),
    
    total_revenue Decimal(18,2),
    total_quantity UInt64,
    total_orders UInt64,
    unique_customers UInt64,
    avg_check Decimal(10,2),
    
    updated_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(month_date)
ORDER BY (month_date, category, region);

CREATE MATERIALIZED VIEW test_db.monthly_sales_mv TO test_db.monthly_sales AS
SELECT
    toStartOfMonth(order_date) AS month_date,
    category,
    region,
    sum(line_total) AS total_revenue,
    sum(quantity) AS total_quantity,
    count() AS total_orders,
    uniq(customer_id) AS unique_customers,
    avg(line_total) AS avg_check,
    now() AS updated_at
FROM test_db.orders_flat
WHERE order_status = 'delivered'
GROUP BY month_date, category, region;