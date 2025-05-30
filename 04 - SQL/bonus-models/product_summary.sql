"""
build the following table in Big Query. You don't have to create the table, just write the code in this file
if you want, you can also create it in a dataset of your name in Big Query - like AMAURY.product_summary
"""

"""
the table must have the following columns:

-- product identifiers and basic attributes
product_id: unique identifier for each product
product_category_name: category the product belongs to
product_weight_g: weight of the product in grams
product_length_cm: length of the product in centimeters
product_height_cm: height of the product in centimeters
product_width_cm: width of the product in centimeters
product_volume_cm3: volume of the product in cubic centimeters
product_photos_qty: number of photos the product has in the catalog

-- sales metrics
total_orders: total number of orders containing this product
total_units_sold: total number of units sold for this product
first_order_date: date of the first order for this product
last_order_date: date of the most recent order for this product
product_lifetime_days: number of days between first and last order
total_revenue: total revenue generated by this product
avg_price: average selling price of the product
total_freight_value: total shipping costs for this product

-- performance metrics
sales_velocity: average number of units sold per day
daily_revenue: average revenue generated per day
days_since_last_order: number of days since the product was last ordered
recent_vs_overall_price_trend: percentage change in recent prices compared to overall average

-- sales trends
best_selling_month: month with highest sales for this product
worst_selling_month: month with lowest sales for this product
avg_monthly_units: average number of units sold per month
sales_volatility: measure of sales variation over time
max_monthly_growth: maximum month-over-month growth rate
min_monthly_growth: minimum month-over-month growth rate

-- category performance
category_sales_rank: rank of the product within its category by sales volume
category_revenue_rank: rank of the product within its category by revenue
category_sales_percentile: percentile rank within category by sales
category_revenue_percentile: percentile rank within category by revenue
category_size: total number of products in the same category

-- seller metrics
unique_sellers_count: number of different sellers offering this product
seller_concentration_index: measure of how concentrated sales are among sellers
top_seller_id: ID of the seller with the most sales for this product
top_seller_units: number of units sold by the top seller
top_seller_share: percentage of total units sold by the top seller

-- customer demographics
unique_customers: number of distinct customers who purchased this product
customer_states_count: number of different states where customers are located
top_customer_state: state with the most customers for this product
repeat_purchase_rate: percentage of customers who purchased the product more than once
new_customer_percent: percentage of orders from first-time customers

-- review metrics
total_reviews: total number of reviews received
avg_review_score: average review score
five_star_percent: percentage of reviews with 5-star rating
one_star_percent: percentage of reviews with 1-star rating
product_sentiment: sentiment category based on review scores (e.g., positive, neutral, negative)
review_rate: percentage of orders that received a review
avg_days_to_review: average time between purchase and first review by order
comment_rate: percentage of reviews with written comments

-- pricing analysis
min_price: minimum recorded price for the product
max_price: maximum recorded price for the product
price_per_kg: price per kilogram (value density)rs
relative_category_price: product price as a percentage of category average

-- delivery performance
avg_delivery_time: average time from order to delivery
avg_delivery_vs_estimated: average difference between actual and estimated delivery
on_time_delivery_rate: percentage of orders delivered on time
cancellation_rate: percentage of orders that were canceled

-- time metrics
etl_timestamp: timestamp when the data was last processed

"""
-- Create a product_summary table in BigQuery
CREATE OR REPLACE TABLE `head-of-data-agelle.Enzo.product_summary` AS

-- Begin with product base data
WITH product_base AS (
  SELECT
    p.product_id,
    p.product_category_name,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    (p.product_length_cm * p.product_height_cm * p.product_width_cm) AS product_volume_cm3,
    p.product_photos_qty
  FROM `head-of-data-agelle`.BRONZE.products AS p
),

-- Order items info with dates, revenue, freight
product_sales AS (
  SELECT
    oi.product_id,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(*) AS total_units_sold,
    MIN(o.order_purchase_timestamp) AS first_order_date,
    MAX(o.order_purchase_timestamp) AS last_order_date,
    DATE_DIFF(DATE(MAX(o.order_purchase_timestamp)), DATE(MIN(o.order_purchase_timestamp)), DAY) AS product_lifetime_days,
    SUM(oi.price) AS total_revenue,
    AVG(oi.price) AS avg_price,
    SUM(oi.freight_value) AS total_freight_value,
    DATE_DIFF(DATE('2018-09-04'), DATE(MAX(o.order_purchase_timestamp)), DAY) AS days_since_last_order
  FROM `head-of-data-agelle`.BRONZE.order_items AS oi
  JOIN `head-of-data-agelle`.BRONZE.orders AS o ON oi.order_id = o.order_id
  GROUP BY oi.product_id
),

-- Seller metrics
seller_stats AS (
  SELECT
    product_id,
    COUNT(DISTINCT seller_id) AS unique_sellers_count,
    APPROX_TOP_COUNT(seller_id, 1)[OFFSET(0)].value AS top_seller_id,
    APPROX_TOP_COUNT(seller_id, 1)[OFFSET(0)].count AS top_seller_units,
    SAFE_DIVIDE(APPROX_TOP_COUNT(seller_id, 1)[OFFSET(0)].count, COUNT(*)) AS top_seller_share,
    SAFE_DIVIDE(SUM(1.0 * 1), COUNT(DISTINCT seller_id)) AS seller_concentration_index
  FROM `head-of-data-agelle`.BRONZE.order_items
  GROUP BY product_id
),

-- Customer reach
customer_demographics AS (
  SELECT
    oi.product_id,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    COUNT(DISTINCT c.customer_state) AS customer_states_count,
    APPROX_TOP_COUNT(c.customer_state, 1)[OFFSET(0)].value AS top_customer_state
  FROM `head-of-data-agelle`.BRONZE.order_items AS oi
  JOIN `head-of-data-agelle`.BRONZE.orders AS o ON oi.order_id = o.order_id
  JOIN `head-of-data-agelle`.BRONZE.customers AS c ON o.customer_id = c.customer_id
  GROUP BY oi.product_id
),

-- Review metrics
product_reviews AS (
  SELECT
    oi.product_id,
    COUNT(r.review_id) AS total_reviews,
    AVG(r.review_score) AS avg_review_score,
    SAFE_DIVIDE(COUNTIF(r.review_score = 5), COUNT(r.review_id)) AS five_star_percent,
    SAFE_DIVIDE(COUNTIF(r.review_score = 1), COUNT(r.review_id)) AS one_star_percent,
    CASE
      WHEN AVG(r.review_score) >= 4 THEN 'positive'
      WHEN AVG(r.review_score) BETWEEN 3 AND 4 THEN 'neutral'
      ELSE 'negative'
    END AS product_sentiment,
    SAFE_DIVIDE(COUNT(DISTINCT r.review_id), COUNT(DISTINCT oi.order_id)) AS review_rate,
    AVG(DATE_DIFF(DATE(r.review_creation_date), DATE(o.order_purchase_timestamp), DAY)) AS avg_days_to_review,
    SAFE_DIVIDE(COUNTIF(r.review_comment_message IS NOT NULL), COUNT(r.review_id)) AS comment_rate
  FROM `head-of-data-agelle`.BRONZE.order_items AS oi
  JOIN `head-of-data-agelle`.BRONZE.orders AS o ON oi.order_id = o.order_id
  JOIN `head-of-data-agelle`.BRONZE.reviews AS r ON oi.order_id = r.order_id
  GROUP BY oi.product_id
),

-- Pricing extremes
price_extremes AS (
  SELECT
    product_id,
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    SAFE_DIVIDE(AVG(price), AVG(NULLIF(product_weight_g, 0))) * 1000 AS price_per_kg
  FROM `head-of-data-agelle`.BRONZE.order_items
  JOIN `head-of-data-agelle`.BRONZE.products USING(product_id)
  GROUP BY product_id
)

-- Final SELECT combining all
SELECT
  pb.product_id,
  pb.product_category_name,
  pb.product_weight_g,
  pb.product_length_cm,
  pb.product_height_cm,
  pb.product_width_cm,
  pb.product_volume_cm3,
  pb.product_photos_qty,

  ps.total_orders,
  ps.total_units_sold,
  ps.first_order_date,
  ps.last_order_date,
  ps.product_lifetime_days,
  ps.total_revenue,
  ps.avg_price,
  ps.total_freight_value,

  SAFE_DIVIDE(ps.total_units_sold, NULLIF(ps.product_lifetime_days, 0)) AS sales_velocity,
  SAFE_DIVIDE(ps.total_revenue, NULLIF(ps.product_lifetime_days, 0)) AS daily_revenue,
  ps.days_since_last_order,
  NULL AS recent_vs_overall_price_trend, -- Placeholder for logic

  NULL AS best_selling_month,
  NULL AS worst_selling_month,
  NULL AS avg_monthly_units,
  NULL AS sales_volatility,
  NULL AS max_monthly_growth,
  NULL AS min_monthly_growth,

  NULL AS category_sales_rank,
  NULL AS category_revenue_rank,
  NULL AS category_sales_percentile,
  NULL AS category_revenue_percentile,
  NULL AS category_size,

  ss.unique_sellers_count,
  ss.seller_concentration_index,
  ss.top_seller_id,
  ss.top_seller_units,
  ss.top_seller_share,

  cd.unique_customers,
  cd.customer_states_count,
  cd.top_customer_state,
  NULL AS repeat_purchase_rate,
  NULL AS new_customer_percent,

  pr.total_reviews,
  pr.avg_review_score,
  pr.five_star_percent,
  pr.one_star_percent,
  pr.product_sentiment,
  pr.review_rate,
  pr.avg_days_to_review,
  pr.comment_rate,

  pe.min_price,
  pe.max_price,
  pe.price_per_kg,
  NULL AS relative_category_price,

  NULL AS avg_delivery_time,
  NULL AS avg_delivery_vs_estimated,
  NULL AS on_time_delivery_rate,
  NULL AS cancellation_rate,

  CURRENT_TIMESTAMP() AS etl_timestamp

FROM product_base pb
LEFT JOIN product_sales ps ON pb.product_id = ps.product_id
LEFT JOIN seller_stats ss ON pb.product_id = ss.product_id
LEFT JOIN customer_demographics cd ON pb.product_id = cd.product_id
LEFT JOIN product_reviews pr ON pb.product_id = pr.product_id
LEFT JOIN price_extremes pe ON pb.product_id = pe.product_id
