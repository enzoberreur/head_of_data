"""
build the following table in Big Query. You don't have to create the table, just write the code in this file
if you want, you can also create it in a dataset of your name in Big Query - like AMAURY.order_summary
"""


"""
the table must have the following columns:

-- order identifiers
order_id: unique identifier for each order
customer_unique_id: unique identifier for the customer who placed the order
order_status: current status of the order (delivered, shipped, canceled, etc.)
order_purchase_timestamp: date and time when the order was placed
order_approved_at: date and time when the order was approved
order_delivered_carrier_date: date and time when the order was handed to the carrier
order_delivered_customer_date: date and time when the order was delivered to the customer
order_estimated_delivery_date: estimated date for delivery provided to customer

-- delivery metrics
actual_delivery_time_days: number of days between purchase and delivery to customer
delivery_delay_days: difference between estimated and actual delivery date
is_delivered_on_time: boolean indicating if order was delivered by the estimated date

-- order items metrics
total_items: total number of items in the order
unique_products: number of different products in the order
unique_sellers: number of different sellers in the order
total_order_value: total monetary value of the order
total_freight_value: total shipping cost for the order
avg_item_price: average price of items in the order
seller_ids_string: comma-separated list of seller IDs involved in the order
product_categories: array of product categories in the order

-- payment metrics
payment_installments_count: number of payment installments
payment_types: types of payment methods used (array)
total_payment_amount: total amount paid
credit_card_amount: amount paid via credit card
voucher_amount: amount paid via voucher
boleto_amount: amount paid via boleto
debit_card_amount: amount paid via debit card

-- review metrics
total_reviews: number of reviews submitted for the order
avg_review_score: average review score
min_review_score: minimum review score
max_review_score: maximum review score
review_titles: array of review titles
review_messages: concatenated review messages
days_to_review: days between purchase and first review
customer_satisfaction_category: category based on review scores (e.g., satisfied, neutral, dissatisfied)

-- customer location metrics
customer_zip_code_prefix: postal code prefix of the customer
customer_city: city of the customer
customer_state: state of the customer
customer_seller_location_match: indicates if customer and seller are in the same state

-- customer order history
is_repeat_order: boolean indicating if this is not the customer's first order
order_sequence_number: the sequence number of this order for the customer
is_repeat_seller_purchase: boolean indicating if customer purchased from this seller before
order_value_vs_customer_avg: difference between order value and customer's average order value

-- state comparative metrics
state_order_value_percentile: percentile of order value within the state
delivery_speed_vs_state_avg: indicates if delivery was faster or slower than state average

-- business categorization
order_value_category: categorization of order based on value (high, medium, low)
gross_profit: order value minus freight cost
shopping_season_category: categorization based on shopping season (christmas, black_friday, easter, regular, etc.)

-- time metrics
etl_timestamp: timestamp when the data was last processed

"""

CREATE OR REPLACE TABLE `head-of-data-agelle.Enzo.order_summary` AS

WITH

-- Base orders with customer details
base_orders AS (
  SELECT
    o.order_id,
    c.customer_unique_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state
  FROM `head-of-data-agelle`.BRONZE.orders AS o
  JOIN `head-of-data-agelle`.BRONZE.customers AS c ON o.customer_id = c.customer_id
),

-- Order items metrics
order_items_agg AS (
  SELECT
    oi.order_id,
    COUNT(*) AS total_items,
    COUNT(DISTINCT oi.product_id) AS unique_products,
    COUNT(DISTINCT oi.seller_id) AS unique_sellers,
    SUM(oi.price) AS total_order_value,
    SUM(oi.freight_value) AS total_freight_value,
    AVG(oi.price) AS avg_item_price,
    STRING_AGG(DISTINCT oi.seller_id, ',') AS seller_ids_string
  FROM `head-of-data-agelle`.BRONZE.order_items AS oi
  GROUP BY oi.order_id
),

-- Product categories per order
product_categories_agg AS (
  SELECT
    oi.order_id,
    ARRAY_AGG(DISTINCT p.product_category_name) AS product_categories
  FROM `head-of-data-agelle`.BRONZE.order_items AS oi
  JOIN `head-of-data-agelle`.BRONZE.products AS p ON oi.product_id = p.product_id
  WHERE p.product_category_name IS NOT NULL
  GROUP BY oi.order_id
),

-- Payment metrics
payment_agg AS (
  SELECT
    p.order_id,
    COUNT(*) AS payment_installments_count,
    ARRAY_AGG(DISTINCT p.payment_type) AS payment_types,
    SUM(p.payment_value) AS total_payment_amount,
    SUM(IF(p.payment_type = 'credit_card', p.payment_value, 0)) AS credit_card_amount,
    SUM(IF(p.payment_type = 'voucher', p.payment_value, 0)) AS voucher_amount,
    SUM(IF(p.payment_type = 'boleto', p.payment_value, 0)) AS boleto_amount,
    SUM(IF(p.payment_type = 'debit_card', p.payment_value, 0)) AS debit_card_amount
  FROM `head-of-data-agelle`.BRONZE.payments AS p
  GROUP BY p.order_id
),

-- Review metrics
review_agg AS (
  SELECT
    r.order_id,
    COUNT(*) AS total_reviews,
    AVG(r.review_score) AS avg_review_score,
    MIN(r.review_score) AS min_review_score,
    MAX(r.review_score) AS max_review_score,
    ARRAY_AGG(DISTINCT r.review_comment_title) AS review_titles,
    STRING_AGG(r.review_comment_message, ' ') AS review_messages,
    MIN(DATE_DIFF(DATE(r.review_creation_date), DATE(o.order_purchase_timestamp), DAY)) AS days_to_review,
    CASE
      WHEN AVG(r.review_score) >= 4 THEN 'satisfied'
      WHEN AVG(r.review_score) BETWEEN 2.5 AND 4 THEN 'neutral'
      ELSE 'dissatisfied'
    END AS customer_satisfaction_category
  FROM `head-of-data-agelle`.BRONZE.reviews AS r
  JOIN `head-of-data-agelle`.BRONZE.orders AS o ON r.order_id = o.order_id
  GROUP BY r.order_id
),

-- Seller-customer state match
seller_customer_state_match AS (
  SELECT
    oi.order_id,
    MAX(IF(c.customer_state = s.seller_state, 1, 0)) AS customer_seller_location_match
  FROM `head-of-data-agelle`.BRONZE.order_items AS oi
  JOIN `head-of-data-agelle`.BRONZE.orders AS o ON oi.order_id = o.order_id
  JOIN `head-of-data-agelle`.BRONZE.customers AS c ON o.customer_id = c.customer_id
  JOIN `head-of-data-agelle`.BRONZE.sellers AS s ON oi.seller_id = s.seller_id
  GROUP BY oi.order_id
),

-- Customer order history
customer_history AS (
  SELECT
    o.order_id,
    c.customer_unique_id,
    RANK() OVER (PARTITION BY c.customer_unique_id ORDER BY o.order_purchase_timestamp) AS order_sequence_number,
    COUNT(*) OVER (PARTITION BY c.customer_unique_id) > 1 AS is_repeat_order
  FROM `head-of-data-agelle`.BRONZE.orders AS o
  JOIN `head-of-data-agelle`.BRONZE.customers AS c ON o.customer_id = c.customer_id
),

-- Customer average order value
customer_avg_order_value AS (
  SELECT
    c.customer_unique_id,
    AVG(oi.price) AS avg_order_value_per_customer
  FROM `head-of-data-agelle`.BRONZE.orders AS o
  JOIN `head-of-data-agelle`.BRONZE.customers AS c ON o.customer_id = c.customer_id
  JOIN `head-of-data-agelle`.BRONZE.order_items AS oi ON o.order_id = oi.order_id
  GROUP BY c.customer_unique_id
),

-- Repeat seller purchase logic
repeat_seller_purchase_raw AS (
  SELECT
    o.order_id,
    o.customer_id,
    oi.seller_id,
    o.order_purchase_timestamp,
    COUNT(*) OVER (
      PARTITION BY o.customer_id, oi.seller_id
      ORDER BY o.order_purchase_timestamp
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS previous_orders
  FROM `head-of-data-agelle`.BRONZE.order_items AS oi
  JOIN `head-of-data-agelle`.BRONZE.orders AS o ON oi.order_id = o.order_id
),
repeat_seller_purchase AS (
  SELECT
    order_id,
    MAX(previous_orders > 0) AS is_repeat_seller_purchase
  FROM repeat_seller_purchase_raw
  GROUP BY order_id
),

-- State comparisons
state_comparisons AS (
  SELECT
    o.order_id,
    c.customer_state,
    PERCENT_RANK() OVER (PARTITION BY c.customer_state ORDER BY SUM(oi.price)) AS state_order_value_percentile,
    DATE_DIFF(DATE(o.order_delivered_customer_date), DATE(o.order_purchase_timestamp), DAY) -
    AVG(DATE_DIFF(DATE(o.order_delivered_customer_date), DATE(o.order_purchase_timestamp), DAY)) OVER (PARTITION BY c.customer_state) AS delivery_speed_vs_state_avg
  FROM `head-of-data-agelle`.BRONZE.orders AS o
  JOIN `head-of-data-agelle`.BRONZE.customers AS c ON o.customer_id = c.customer_id
  JOIN `head-of-data-agelle`.BRONZE.order_items AS oi ON o.order_id = oi.order_id
  GROUP BY o.order_id, c.customer_state, o.order_delivered_customer_date, o.order_purchase_timestamp
)

-- Final assembly
SELECT
  b.order_id,
  b.customer_unique_id,
  b.order_status,
  b.order_purchase_timestamp,
  b.order_approved_at,
  b.order_delivered_carrier_date,
  b.order_delivered_customer_date,
  b.order_estimated_delivery_date,

  DATE_DIFF(DATE(b.order_delivered_customer_date), DATE(b.order_purchase_timestamp), DAY) AS actual_delivery_time_days,
  DATE_DIFF(DATE(b.order_delivered_customer_date), DATE(b.order_estimated_delivery_date), DAY) AS delivery_delay_days,
  DATE_DIFF(DATE(b.order_delivered_customer_date), DATE(b.order_estimated_delivery_date), DAY) <= 0 AS is_delivered_on_time,

  oi.total_items,
  oi.unique_products,
  oi.unique_sellers,
  oi.total_order_value,
  oi.total_freight_value,
  oi.avg_item_price,
  oi.seller_ids_string,
  pc.product_categories,

  p.payment_installments_count,
  p.payment_types,
  p.total_payment_amount,
  p.credit_card_amount,
  p.voucher_amount,
  p.boleto_amount,
  p.debit_card_amount,

  r.total_reviews,
  r.avg_review_score,
  r.min_review_score,
  r.max_review_score,
  r.review_titles,
  r.review_messages,
  r.days_to_review,
  r.customer_satisfaction_category,

  b.customer_zip_code_prefix,
  b.customer_city,
  b.customer_state,
  scm.customer_seller_location_match = 1 AS customer_seller_location_match,

  ch.is_repeat_order,
  ch.order_sequence_number,
  rsp.is_repeat_seller_purchase,
  oi.total_order_value - ca.avg_order_value_per_customer AS order_value_vs_customer_avg,

  sc.state_order_value_percentile,
  sc.delivery_speed_vs_state_avg,

  CASE
    WHEN oi.total_order_value >= 300 THEN 'high'
    WHEN oi.total_order_value BETWEEN 100 AND 299.99 THEN 'medium'
    ELSE 'low'
  END AS order_value_category,

  oi.total_order_value - oi.total_freight_value AS gross_profit,

  CASE
    WHEN EXTRACT(MONTH FROM b.order_purchase_timestamp) = 11 THEN 'black_friday'
    WHEN EXTRACT(MONTH FROM b.order_purchase_timestamp) = 12 THEN 'christmas'
    WHEN EXTRACT(MONTH FROM b.order_purchase_timestamp) = 4 THEN 'easter'
    ELSE 'regular'
  END AS shopping_season_category,

  CURRENT_TIMESTAMP() AS etl_timestamp

FROM base_orders AS b
LEFT JOIN order_items_agg AS oi ON b.order_id = oi.order_id
LEFT JOIN product_categories_agg AS pc ON b.order_id = pc.order_id
LEFT JOIN payment_agg AS p ON b.order_id = p.order_id
LEFT JOIN review_agg AS r ON b.order_id = r.order_id
LEFT JOIN seller_customer_state_match AS scm ON b.order_id = scm.order_id
LEFT JOIN customer_history AS ch ON b.order_id = ch.order_id
LEFT JOIN customer_avg_order_value AS ca ON b.customer_unique_id = ca.customer_unique_id
LEFT JOIN repeat_seller_purchase AS rsp ON b.order_id = rsp.order_id
LEFT JOIN state_comparisons AS sc ON b.order_id = sc.order_id