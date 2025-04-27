-- Answer all the questions below with aggegate SQL queries
-- don't forget to add a screenshot of the result from BigQuery directly in the basics/ folder

-- 1. What was the total revenue and order count for 2018?

SELECT SUM(payments.payment_value), COUNT(orders.order_id)
FROM `BRONZE.payments` as payments
JOIN `BRONZE.orders` as orders
ON payments.order_id = orders.order_id
WHERE order_purchase_timestamp BETWEEN '2018-01-01' AND '2018-12-01';

-- 2. What is the total_sales, average_order_sales, and first_order_date by customer? 
-- Round the values to 2 decimal places & order by total_sales descending
-- limit to 1000 results

SELECT
  customers.customer_unique_id,
  ROUND(SUM(payments.payment_value), 2) AS total_sales,
  ROUND(AVG(payments.payment_value), 2) AS average_order_sales,
  MIN(orders.order_purchase_timestamp) AS first_order_date
FROM
  `head-of-data-agelle`.BRONZE.customers AS customers
  INNER JOIN
  `head-of-data-agelle`.BRONZE.orders AS orders
  ON customers.customer_id = orders.customer_id
  INNER JOIN
  `head-of-data-agelle`.BRONZE.payments AS payments
  ON orders.order_id = payments.order_id
GROUP BY 1
ORDER BY total_sales DESC
LIMIT 1000;

-- 3. Who are the top 10 most successful sellers?

SELECT
  sellers.seller_id,
  SUM(order_items.price + order_items.freight_value) AS total_revenue
FROM
  `head-of-data-agelle`.BRONZE.sellers AS sellers
  INNER JOIN
  `head-of-data-agelle`.BRONZE.order_items AS order_items
  ON sellers.seller_id = order_items.seller_id
GROUP BY 1
ORDER BY total_revenue DESC
LIMIT 10;

-- 4. What’s the preferred payment method by product category?

SELECT
    p.product_category_name,
    modes.payment_type,
    COUNT(o.order_id) AS order_count
  FROM
    `head-of-data-agelle.BRONZE.orders` AS o
    JOIN `head-of-data-agelle.BRONZE.payments` AS modes ON o.order_id = modes.order_id
    JOIN `head-of-data-agelle.BRONZE.order_items` AS oi ON o.order_id = oi.order_id
    JOIN `head-of-data-agelle.BRONZE.products` AS p ON oi.product_id = p.product_id
  GROUP BY
    1,
    2
ORDER BY
  p.product_category_name,
  order_count DESC

