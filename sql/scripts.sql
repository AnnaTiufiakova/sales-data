-- DATA MANIPULATION IN SNOWFLAKE
-- Creating a view 
CREATE OR REPLACE VIEW sales_view AS
SELECT
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    
    i.order_item_id,
    i.product_id,
    i.seller_id,
    i.shipping_limit_date,
    i.price,
    i.freight_value,

    p.payment_type,
    p.payment_installments,
    p.payment_value,

    pr.product_category,
    pr.product_name_length,
    pr.product_weight_g
FROM
    orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_items i ON o.order_id = i.order_id
LEFT JOIN payments p ON o.order_id = p.order_id
LEFT JOIN products pr ON i.product_id = pr.product_id;

--10 Top-selling Product Categories
SELECT 
    product_category,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(price) AS total_sales
FROM sales_view
GROUP BY product_category
ORDER BY total_sales DESC
LIMIT 10;

--Delivery Performace by State
SELECT 
    customer_state,
    ROUND(AVG(DATEDIFF('day', order_purchase_timestamp, order_delivered_customer_date)), 2) AS avg_delivery_days
FROM sales_view
WHERE order_delivered_customer_date IS NOT NULL
GROUP BY customer_state
ORDER BY avg_delivery_days;

--Revenue Breakdown by Payment Type
SELECT 
    payment_type,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(payment_value) AS total_payment_value
FROM sales_view
GROUP BY payment_type
ORDER BY total_payment_value DESC;

--Average Order Value and Freight per State
SELECT 
    customer_state,
    ROUND(AVG(price), 2) AS avg_order_value,
    ROUND(AVG(freight_value), 2) AS avg_freight
FROM sales_view
GROUP BY customer_state
ORDER BY avg_order_value DESC;

--Monthly Sales Trends
SELECT 
    TO_CHAR(order_purchase_timestamp, 'YYYY-MM') AS order_month,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(price), 2) AS total_sales
FROM sales_view
GROUP BY order_month
ORDER BY order_month;
