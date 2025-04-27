-- Answer all the questions below with basics SQL queries
-- don't forget to add a screenshot of the result from BigQuery directly in the basics/ folder

-- 1. What are the possible values of an order status? 

SELECT distinct order_status
FROM `BRONZE.orders`;

-- 2. Who are the 5 last customers that purchased a DELIVERED order (order with status DELIVERED)?
-- print their customer_id, their unique_id, and city

SELECT
  customers.customer_id,
  customers.customer_unique_id,
  customers.customer_city
FROM
  `head-of-data-agelle`.BRONZE.customers AS customers
  INNER JOIN
  `head-of-data-agelle`.BRONZE.orders AS orders
  ON customers.customer_id = orders.customer_id
WHERE
  orders.order_status = 'delivered'
LIMIT 5;

-- 3. Add a column is_sp which returns 1 if the customer is from São Paulo and 0 otherwise

SELECT
  customers.customer_id,
  customers.customer_unique_id,
  customers.customer_city,
  `IF`(customers.customer_city = 'São Paulo', 1, 0) AS is_sp
FROM
  `head-of-data-agelle`.BRONZE.customers AS customers
  INNER JOIN
  `head-of-data-agelle`.BRONZE.orders AS orders
  ON customers.customer_id = orders.customer_id
WHERE
  orders.order_status = 'delivered'
LIMIT 5;

-- 4. add a new column: what's the product category associated to the order?

SELECT order_items.order_id, products.product_category_name
FROM `BRONZE.order_items` as order_items
INNER JOIN `BRONZE.products` as products
ON order_items.product_id = products.product_id;

