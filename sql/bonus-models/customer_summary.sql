"""
build the following table in Big Query. You don't have to create the table, just write the code in this file
if you want, you can also create it in a dataset of your name in Big Query - like AMAURY.customer_summary
"""


"""
the table must have the following columns:

-- customer identifiers
customer_unique_id: unique identifier for each customer
customer_zip_code_prefix: customer's postal code prefix
customer_city: city where the customer is located
customer_state: state where the customer is located

-- order metrics
total_orders: total number of orders placed by the customer
first_order_date: date of the customer's first purchase
last_order_date: date of the customer's most recent purchase
customer_lifetime_days: number of days between first and last order
delivered_orders: number of successfully delivered orders
canceled_orders: number of canceled orders
cancellation_rate: percentage of orders that were canceled

-- purchase patterns
avg_days_between_orders: average time between consecutive orders
days_since_last_order: number of days since the customer's most recent purchase (with current date = 2018-09-04, when the data stops)

-- customer life time value / cost metrics
total_spend: total amount spent by the customer
avg_order_value: average monetary value per order
total_freight_cost: total shipping costs paid
freight_to_price_ratio: shipping cost as a percentage of product price

-- product metrics
unique_products_purchased: number of different products bought
unique_categories_count: number of different product categories explored
top_categories: most frequently purchased product categories

-- payment behavior
payment_methods_count: number of different payment methods used
primary_payment_method: most frequently used payment method
avg_installments: average number of installments used for payments

-- review behavior
total_reviews: number of reviews submitted
avg_review_score: average rating given in reviews
review_rate: percentage of orders that received a review
five_star_percent: percentage of reviews with 5-star rating
one_star_percent: percentage of reviews with 1-star rating
avg_days_to_review: average time between purchase and first review submission by order
customer_sentiment: overall sentiment category based on reviews (e.g., positive, neutral, negative)

-- delivery experience
avg_delivery_time: average time from order to delivery
avg_delivery_vs_estimated: average difference between actual and estimated delivery dates
on_time_delivery_rate: percentage of orders delivered on or before estimated date

-- seller relationships
unique_sellers: number of different sellers purchased from
same_state_purchase_percent: percentage of purchases from sellers in the same state

-- rfm analysis (rfm = recency, frequency, monetary)
recency_days: days since last purchase (with current date = 2018-09-04)
frequency: number of purchases
recency_score: score based on recency (1-5 scale)
frequency_score: score based on frequency (1-5 scale)
monetary_score: score based on monetary value (1-5 scale)
rfm_score: combined rfm score
customer_segment: customer segment based on rfm analysis (Champions, Promising, Recent Customers, At Risk, Lost, Other)

-- time metrics
acquisition_year: year when the customer made their first purchase
acquisition_quarter: quarter when the customer made their first purchase
acquisition_month: month when the customer made their first purchase
etl_timestamp: timestamp when the data was last processed


"""

CREATE OR REPLACE TABLE `head-of-data-agelle.Enzo.customer_summary` AS

WITH
-- (same CTEs as before, unchanged)
order_items_with_seller AS (
  SELECT
    oi.order_id,
    oi.product_id,
    oi.seller_id,
    oi.price,
    oi.freight_value
  FROM `head-of-data-agelle`.BRONZE.order_items AS oi
),
customer_orders AS (
  SELECT
    o.customer_id,
    o.order_id,
    o.order_purchase_timestamp,
    o.order_status,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date
  FROM `head-of-data-agelle`.BRONZE.orders AS o
),
customer_payments AS (
  SELECT
    p.order_id,
    p.payment_type,
    p.payment_installments,
    p.payment_value
  FROM `head-of-data-agelle`.BRONZE.payments AS p
),
customer_reviews AS (
  SELECT
    r.order_id,
    r.review_score,
    r.review_creation_date,
    r.review_id
  FROM `head-of-data-agelle`.BRONZE.reviews AS r
),
customer_products AS (
  SELECT
    oi.order_id,
    p.product_category_name
  FROM order_items_with_seller AS oi
  JOIN `head-of-data-agelle`.BRONZE.products AS p
    ON oi.product_id = p.product_id
),
order_date_diffs AS (
  SELECT
    customer_id,
    order_purchase_timestamp,
    DATE_DIFF(DATE(order_purchase_timestamp),
              DATE(LAG(order_purchase_timestamp) OVER (PARTITION BY customer_id ORDER BY order_purchase_timestamp)),
              DAY) AS days_since_last
  FROM customer_orders
),
customer_category_counts AS (
  SELECT
    c.customer_unique_id,
    p.product_category_name,
    COUNT(*) AS category_count
  FROM `head-of-data-agelle`.BRONZE.customers AS c
  JOIN `head-of-data-agelle`.BRONZE.orders AS o ON c.customer_id = o.customer_id
  JOIN `head-of-data-agelle`.BRONZE.order_items AS oi ON o.order_id = oi.order_id
  JOIN `head-of-data-agelle`.BRONZE.products AS p ON oi.product_id = p.product_id
  GROUP BY c.customer_unique_id, p.product_category_name
),
customer_primary_payment AS (
  SELECT
    c.customer_unique_id,
    cp.payment_type,
    COUNT(*) AS payment_count,
    ROW_NUMBER() OVER (
      PARTITION BY c.customer_unique_id
      ORDER BY COUNT(*) DESC
    ) AS rn
  FROM `head-of-data-agelle`.BRONZE.customers AS c
  JOIN `head-of-data-agelle`.BRONZE.orders AS o ON c.customer_id = o.customer_id
  JOIN `head-of-data-agelle`.BRONZE.payments AS cp ON o.order_id = cp.order_id
  GROUP BY c.customer_unique_id, cp.payment_type
),

-- ============ Main Aggregation ============
customer_metrics AS (
  SELECT
    -- Identifiers
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,

    -- Order metrics
    COUNT(DISTINCT co.order_id) AS total_orders,
    MIN(co.order_purchase_timestamp) AS first_order_date,
    MAX(co.order_purchase_timestamp) AS last_order_date,
    DATE_DIFF(DATE(MAX(co.order_purchase_timestamp)), DATE(MIN(co.order_purchase_timestamp)), DAY) AS customer_lifetime_days,
    COUNTIF(co.order_status = 'delivered') AS delivered_orders,
    COUNTIF(co.order_status = 'canceled') AS canceled_orders,
    SAFE_DIVIDE(COUNTIF(co.order_status = 'canceled'), COUNT(DISTINCT co.order_id)) AS cancellation_rate,

    -- Purchase patterns
    AVG(odd.days_since_last) AS avg_days_between_orders,
    DATE_DIFF(DATE '2018-09-04', DATE(MAX(co.order_purchase_timestamp)), DAY) AS days_since_last_order,

    -- LTV / cost metrics
    SUM(cp.payment_value) AS total_spend,
    SAFE_DIVIDE(SUM(cp.payment_value), COUNT(DISTINCT co.order_id)) AS avg_order_value,
    SUM(oi.freight_value) AS total_freight_cost,
    SAFE_DIVIDE(SUM(oi.freight_value), SUM(oi.price)) AS freight_to_price_ratio,

    -- Product metrics
    COUNT(DISTINCT oi.product_id) AS unique_products_purchased,
    COUNT(DISTINCT cp2.product_category_name) AS unique_categories_count,
    ARRAY_AGG(STRUCT(cat.product_category_name, cat.category_count)
              ORDER BY cat.category_count DESC LIMIT 3) AS top_categories,

    -- Payment behavior
    COUNT(DISTINCT cp.payment_type) AS payment_methods_count,
    AVG(cp.payment_installments) AS avg_installments,

    -- Review behavior
    COUNT(DISTINCT cr.review_id) AS total_reviews,
    AVG(cr.review_score) AS avg_review_score,
    SAFE_DIVIDE(COUNT(DISTINCT cr.review_id), COUNT(DISTINCT co.order_id)) AS review_rate,
    SAFE_DIVIDE(COUNTIF(cr.review_score = 5), COUNT(DISTINCT cr.review_id)) AS five_star_percent,
    SAFE_DIVIDE(COUNTIF(cr.review_score = 1), COUNT(DISTINCT cr.review_id)) AS one_star_percent,
    AVG(IF(cr.review_creation_date IS NOT NULL,
           DATE_DIFF(DATE(cr.review_creation_date), DATE(co.order_purchase_timestamp), DAY), NULL)) AS avg_days_to_review,
    CASE
      WHEN AVG(cr.review_score) > 4 THEN 'Positive'
      WHEN AVG(cr.review_score) BETWEEN 3 AND 4 THEN 'Neutral'
      ELSE 'Negative'
    END AS customer_sentiment,

    -- Delivery experience
    AVG(DATE_DIFF(DATE(co.order_delivered_customer_date), DATE(co.order_purchase_timestamp), DAY)) AS avg_delivery_time,
    AVG(DATE_DIFF(DATE(co.order_delivered_customer_date), DATE(co.order_estimated_delivery_date), DAY)) AS avg_delivery_vs_estimated,
    SAFE_DIVIDE(COUNTIF(DATE_DIFF(DATE(co.order_delivered_customer_date), DATE(co.order_estimated_delivery_date), DAY) <= 0),
                COUNT(DISTINCT co.order_id)) AS on_time_delivery_rate,

    -- Seller relationships
    COUNT(DISTINCT oi.seller_id) AS unique_sellers,
    SAFE_DIVIDE(COUNTIF(s.seller_state = c.customer_state), COUNT(DISTINCT oi.seller_id)) AS same_state_purchase_percent,

    -- RFM components
    DATE_DIFF(DATE '2018-09-04', DATE(MAX(co.order_purchase_timestamp)), DAY) AS recency_days,
    COUNT(DISTINCT co.order_id) AS frequency,
    SUM(cp.payment_value) AS monetary,

    -- Acquisition
    EXTRACT(YEAR FROM MIN(co.order_purchase_timestamp)) AS acquisition_year,
    EXTRACT(QUARTER FROM MIN(co.order_purchase_timestamp)) AS acquisition_quarter,
    EXTRACT(MONTH FROM MIN(co.order_purchase_timestamp)) AS acquisition_month,

    -- ETL timestamp
    CURRENT_TIMESTAMP() AS etl_timestamp

  FROM `head-of-data-agelle`.BRONZE.customers AS c
  JOIN customer_orders AS co ON c.customer_id = co.customer_id
  LEFT JOIN order_date_diffs AS odd ON co.customer_id = odd.customer_id AND co.order_purchase_timestamp = odd.order_purchase_timestamp
  LEFT JOIN customer_payments AS cp ON co.order_id = cp.order_id
  LEFT JOIN customer_reviews AS cr ON co.order_id = cr.order_id
  LEFT JOIN order_items_with_seller AS oi ON co.order_id = oi.order_id
  LEFT JOIN customer_products AS cp2 ON co.order_id = cp2.order_id
  LEFT JOIN `head-of-data-agelle`.BRONZE.sellers AS s ON oi.seller_id = s.seller_id
  LEFT JOIN customer_category_counts AS cat ON c.customer_unique_id = cat.customer_unique_id

  GROUP BY
    c.customer_unique_id, c.customer_zip_code_prefix, c.customer_city, c.customer_state
)

-- Final select: add primary_payment_method and RFM scores
SELECT
  cm.*,
  cpp.payment_type AS primary_payment_method,

  -- RFM scores
  NTILE(5) OVER (ORDER BY recency_days DESC) AS recency_score,
  NTILE(5) OVER (ORDER BY frequency) AS frequency_score,
  NTILE(5) OVER (ORDER BY monetary) AS monetary_score,

  -- Combined RFM score
  CONCAT(
    CAST(NTILE(5) OVER (ORDER BY recency_days DESC) AS STRING),
    CAST(NTILE(5) OVER (ORDER BY frequency) AS STRING),
    CAST(NTILE(5) OVER (ORDER BY monetary) AS STRING)
  ) AS rfm_score,

  -- Customer segment
  CASE
    WHEN NTILE(5) OVER (ORDER BY recency_days DESC) = 5 AND
         NTILE(5) OVER (ORDER BY frequency) = 5 AND
         NTILE(5) OVER (ORDER BY monetary) = 5 THEN 'Champions'
    WHEN CONCAT(
      CAST(NTILE(5) OVER (ORDER BY recency_days DESC) AS STRING),
      CAST(NTILE(5) OVER (ORDER BY frequency) AS STRING),
      CAST(NTILE(5) OVER (ORDER BY monetary) AS STRING)
    ) IN ('554', '545', '455') THEN 'Promising'
    WHEN NTILE(5) OVER (ORDER BY recency_days DESC) = 5 THEN 'Recent Customers'
    WHEN NTILE(5) OVER (ORDER BY frequency) = 1 AND
         NTILE(5) OVER (ORDER BY monetary) = 1 THEN 'At Risk'
    WHEN NTILE(5) OVER (ORDER BY frequency) = 1 AND
         NTILE(5) OVER (ORDER BY monetary) = 1 AND
         NTILE(5) OVER (ORDER BY recency_days DESC) = 1 THEN 'Lost'
    ELSE 'Other'
  END AS customer_segment

FROM customer_metrics cm
LEFT JOIN customer_primary_payment cpp
  ON cm.customer_unique_id = cpp.customer_unique_id AND cpp.rn = 1;
