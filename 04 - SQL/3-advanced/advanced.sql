-- Answer all the questions below with advanced SQL queries (partitioning, CASE WHENs)
-- don't forget to add a screenshot of the result from BigQuery directly in the basics/ folder

-- 1. Where are located the clients that ordered more than the average?

select distinct A.customer_city, B.customer_state,
count(distinct B.order_id) as order_count
from `BRONZE.customers` A
inner join `BRONZE.orders` B
on A.customer_id = B.customer_id
group by A.customer_city, A.customer_state
having count(distinct B.order_id) > (
  select avg(order_count) 
  from (
    select customer_id, count(distinct order_id) as order_count 
    from `BRONZE.orders` 
    group by customer_id
  )
)
order by order_count desc


-- 2. Segment clients in categories based on the amount spent (use CASE WHEN)
select 
  A.customer_id,
  sum(B.payment_value) as total_spent,
  case
    when sum(B.payment_value) >= 1000 then 'High Value'
    when sum(B.payment_value) >= 500 then 'Medium Value' 
    when sum(B.payment_value) >= 100 then 'Low Value'
    else 'Very Low Value'
  end as customer_segment
from `BRONZE.orders` A
join `BRONZE.payments` B 
on A.order_id = B.order_id
group by A.customer_id
order by total_spent desc


-- 3. Compute the difference in days between the first and last order of a client. Compute then the average (use PARTITION BY)

WITH customer_orders AS (
  SELECT 
    customer_id,
    MIN(order_purchase_timestamp) OVER (PARTITION BY customer_id) as first_order,
    MAX(order_purchase_timestamp) OVER (PARTITION BY customer_id) as last_order,
    DATE_DIFF(MAX(order_purchase_timestamp) OVER (PARTITION BY customer_id),
              MIN(order_purchase_timestamp) OVER (PARTITION BY customer_id), DAY) as days_between_orders
  FROM `BRONZE.orders`
)
SELECT 
  customer_id,
  first_order,
  last_order,
  days_between_orders,
  AVG(days_between_orders) OVER() as avg_days_between_orders
FROM customer_orders
ORDER BY days_between_orders DESC

-- 4. Add a column to the query in basics question 2.: what was their first product category purchased?

select 
  A.customer_id,
  sum(B.payment_value) as total_spent,
  case
    when sum(B.payment_value) >= 1000 then 'High Value'
    when sum(B.payment_value) >= 500 then 'Medium Value' 
    when sum(B.payment_value) >= 100 then 'Low Value'
    else 'Very Low Value'
  end as customer_segment
from `BRONZE.orders` A
join `BRONZE.payments` B
on A.order_id = B.order_id

------------------------------------



