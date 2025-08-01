-- Databricks notebook source
-- MAGIC %md
-- MAGIC **NANDINI DEVALLA AND RUPALI KAKADIA**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## **1.Data Exploration & Preprocessing**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **1. Load all datasets (orders, payments, customers, products, reviews) into Spark DataFrames.**

-- COMMAND ----------

-- Orders Table (Partitioned by order_status)
CREATE TABLE orders (
  order_id STRING,
  customer_id STRING,
  order_status STRING,
  order_purchase_timestamp TIMESTAMP,
  order_delivered_customer_date TIMESTAMP
)
USING PARQUET
PARTITIONED BY (order_status);

-- Payments Table (Partitioned by payment_type)
CREATE TABLE payments (
  order_id STRING,
  payment_sequential INT,
  payment_type STRING,
  payment_installments INT,
  payment_value DOUBLE
)
USING PARQUET
PARTITIONED BY (payment_type);

-- Customers Table (Partitioned by customer_state)
CREATE TABLE customers (
  customer_id STRING,
  customer_unique_id STRING,
  customer_city STRING,
  customer_state STRING
)
USING PARQUET
PARTITIONED BY (customer_state);

-- Products Table (Partitioned by product_category_name)
CREATE TABLE products (
  product_id STRING,
  product_category_name STRING,
  product_name_length INT,
  product_description_length INT,
  product_photos_qty INT,
  product_weight_g INT,
  product_length_cm INT,
  product_height_cm INT,
  product_width_cm INT
)
USING PARQUET
PARTITIONED BY (product_category_name);

-- Order Items Table (Partitioned by product_category_name)
CREATE TABLE order_items (
  order_id STRING,
  order_item_id INT,
  product_id STRING,
  seller_id STRING,
  shipping_limit_date TIMESTAMP,
  price DOUBLE,
  freight_value DOUBLE,
  product_category_name STRING
)
USING PARQUET
PARTITIONED BY (product_category_name);

--Reviews Table (Partitioned by review_score)
CREATE TABLE reviews (
  review_id STRING,
  order_id STRING,
  review_score INT,
  review_creation_date TIMESTAMP,
  review_comment_message STRING
)
USING PARQUET
PARTITIONED BY (review_score);

-- Sellers Table (Partitioned by seller_state)
CREATE TABLE sellers (
  seller_id STRING,
  seller_city STRING,
  seller_state STRING
)
USING PARQUET
PARTITIONED BY (seller_state);

-- Geolocation Table
CREATE TABLE geolocation (
  geolocation_zip_code_prefix STRING,
  geolocation_lat DOUBLE,
  geolocation_lng DOUBLE,
  geolocation_city STRING,
  geolocation_state STRING
)
USING PARQUET
PARTITIONED BY (geolocation_state);

--Product Category Translation Table
CREATE TABLE product_category_translation (
  product_category_name STRING,
  product_category_name_english STRING
)
USING PARQUET;

-- COMMAND ----------

-- Checking the first few rows of each dataset
SELECT * FROM orders LIMIT 5;
SELECT * FROM payments LIMIT 5;
SELECT * FROM customers LIMIT 5;
SELECT * FROM products LIMIT 5;
SELECT * FROM reviews LIMIT 5;
SELECT * FROM sellers LIMIT 5;
SELECT * FROM geolocation LIMIT 5;
SELECT * FROM order_items LIMIT 5;
SELECT * FROM category_translation LIMIT 5;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC **2.	Display schema and sample records.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _**Orders Table**_

-- COMMAND ----------

-- Schema for orders
describe orders;

-- COMMAND ----------

--First 5 rows of orders table
select * from orders limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _**Payments Table**_

-- COMMAND ----------

--Schema
describe payments;

-- COMMAND ----------

--First 5 rows
select * from payments limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _**Customers Table**_

-- COMMAND ----------

--Schema
describe customers;

-- COMMAND ----------

--First 5 rows
select * from customers limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _**Products Table**_

-- COMMAND ----------

--Schema
describe products;

-- COMMAND ----------

--First 5 rows
select * from products limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _**Reviews Table**_
-- MAGIC

-- COMMAND ----------

--Schema
describe reviews;

-- COMMAND ----------

--First five rows
select * from reviews limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _**Sellers Table**_
-- MAGIC

-- COMMAND ----------

--Schema
describe sellers;

-- COMMAND ----------

--First five rows
select * from sellers limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _**Geolocation Table**_
-- MAGIC

-- COMMAND ----------

--Schemaa
describe geolocation;

-- COMMAND ----------

--First five rows
select * from geolocation limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _**Order Items Table**_
-- MAGIC

-- COMMAND ----------

--Schema
describe order_items;

-- COMMAND ----------

--First five rows
select * from order_items limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _**Category Translation Table**_

-- COMMAND ----------

--Schema
describe product_category_translation;

-- COMMAND ----------

--First five rows
select * from product_category_translation limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **3.	Count the number of unique orders and customers.**

-- COMMAND ----------

--Unique Orders
select count(distinct order_id) as unique_orders 
from orders;

-- COMMAND ----------

select count(customer_id) as total_customers, 
       count(distinct customer_unique_id) as unique_customers
from customers;

-- COMMAND ----------

select count(distinct customer_unique_id) as unique_customers 
from customers;

-- COMMAND ----------

select customer_id, count(order_id) as total_orders
from orders
group by customer_id
order by total_orders desc
limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##**2. Sales & Revenue Analysis**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **4.	Calculate total revenue, average order value, and monthly revenue trends.**

-- COMMAND ----------

--Total Revenue
select sum(payment_value) as total_revenue 
from payments;

-- COMMAND ----------

select sum(price) as total_revenue
from order_items;

-- COMMAND ----------

--checking for if there are multiple records for any order
select order_id, 
       count(*) as payment_count
from payments
group by order_id
having count(*) > 1
order by payment_count desc;

-- COMMAND ----------

--comparing sum of payments vs order items
select p.order_id, 
       sum(p.payment_value) as total_paid, 
       sum(oi.price + oi.freight_value) as total_order_cost
from payments p
join order_items oi on p.order_id = oi.order_id
group by p.order_id
having sum(p.payment_value) != sum(oi.price + oi.freight_value)
order by abs(sum(p.payment_value) - sum(oi.price + oi.freight_value)) desc;


-- COMMAND ----------

--Checking for the payment types used
select payment_type, 
       count(*) as payment_count
from payments
where order_id in (
    select order_id from payments group by order_id having count(*) > 1
)
group by payment_type
order by payment_count desc;

-- COMMAND ----------

select sum(price + freight_value) as total_revenue
from order_items;

-- COMMAND ----------

select order_id, count(*) as payment_count
from payments
group by order_id
having count(*) > 1
order by payment_count desc;

-- COMMAND ----------

select 
    (select count(distinct order_id) from orders) as total_orders,
    (select count(distinct order_id) from order_items) as orders_in_order_items;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Rationale:**
-- MAGIC
-- MAGIC Payment_value represents the actual transaction value of an order. It can contain mutiple transactions for the same order as it also includes installments, vouchers etc. Which means, a single order can appear multiple times in the payments table even though its just one sale.Becuase of this, taking a sum of **payment_value** for **total_revenue can inflate our final value.**
-- MAGIC
-- MAGIC The order_items table has the actual values of goods that were sold. There is no duplication of transactions. So taking a **sum of price and freight_value** can give us a better figure of total_revenue.
-- MAGIC
-- MAGIC **But on a whole, there is a very small difference in both the values which could be due to some orders missing from order_items table. Payment table has 99441 order whereas order_items has 98666 orders. So in this case payment_value might be giving us the actual total_revenue instead of inflating it**

-- COMMAND ----------

--Average Order Value
select sum(payment_value) / count(distinct order_id) as average_order_value from payments;

-- COMMAND ----------

--If we use order_items for Average order value
select sum(price + freight_value) / count(distinct order_id) as avg_order_value
from order_items;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **The average order value is same for both the calculations**

-- COMMAND ----------

--Montly Revenue Trends
select year(o.order_purchase_timestamp) as year,
       month(o.order_purchase_timestamp) as month,
       sum(p.payment_value) as total_revenue
from orders o
join payments p on o.order_id = p.order_id
group by year, month
order by year, month;

-- COMMAND ----------

select year(o.order_purchase_timestamp) as year,
       month(o.order_purchase_timestamp) as month,
       sum(oi.price + oi.freight_value) as monthly_revenue
from orders o
join order_items oi on o.order_id = oi.order_id
group by year, month
order by year, month;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **5.	Identify the top 5 best-selling product categories.**

-- COMMAND ----------

--Top 5 categories based on total revenue 
select pct.product_category_name_english as category,
       sum(oi.price * oi.order_item_id) as total_sales
from order_items oi
join products p on oi.product_id = p.product_id
join product_category_translation pct on p.product_category_name = pct.product_category_name
group by pct.product_category_name_english
order by total_sales desc
limit 5;

-- COMMAND ----------

--Top 5 best selling product categories based on number of units sold
select pct.product_category_name_english as category,
       sum(oi.order_item_id) as total_units_sold
from order_items oi
join products p on oi.product_id = p.product_id
join product_category_translation pct on p.product_category_name = pct.product_category_name
group by pct.product_category_name_english
order by total_units_sold desc
limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **6.	Determine the top 3 payment methods used by customers.**

-- COMMAND ----------

--Top 3 payment methods
select payment_type, count(*) as usage_count
from payments
group by payment_type
order by usage_count desc
limit 3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##**3. Customer Behavior Analysis**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **7.	Find the average number of orders per customer.**

-- COMMAND ----------

select count(distinct o.order_id) / count(distinct c.customer_unique_id) as avg_orders_per_customer
from orders o
join customers c on o.customer_id = c.customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **8.	Identify the top 5 cities with the highest number of purchases.**

-- COMMAND ----------

--Top 5 cities with most purchases
select c.customer_city, 
       count(o.order_id) as total_orders
from customers c
join orders o on c.customer_id = o.customer_id
group by c.customer_city
order by total_orders desc
limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **9.	Segment customers based on their purchase frequency (one-time, occasional (2 – 5 orders), frequent (6 – 15 orders) and loyal (> 15 orders)).**

-- COMMAND ----------

--Customer segmentation based on purchase frequency
select case 
           when order_count = 1 then 'one-time'
           when order_count between 2 and 5 then 'occasional'
           when order_count between 6 and 15 then 'frequent'
           else 'loyal'
       end as customer_segment,
       count(*) as customer_count
from (
    select c.customer_unique_id, 
           count(o.order_id) as order_count
    from customers c
    join orders o on c.customer_id = o.customer_id
    group by c.customer_unique_id
) order_counts
group by customer_segment
order by customer_count desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##**4. Delivery & Logistics Analysis**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **10.	Calculate the average shipping time per order.**

-- COMMAND ----------

--avg shipping time per order
select avg(datediff(order_delivered_customer_date, order_purchase_timestamp)) as avg_shipping_time
from orders
where order_delivered_customer_date is not null;

-- COMMAND ----------

--In hours
select avg(unix_timestamp(order_delivered_customer_date) - unix_timestamp(order_purchase_timestamp)) / 3600 as avg_shipping_hours
from orders
where order_delivered_customer_date is not null;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **11.	 Identify the Top 10 Orders with the Worst Delivery Delays**

-- COMMAND ----------

select o.order_id,
       datediff(o.order_delivered_customer_date, o.order_estimated_delivery_date) as delay_days
from orders o
where o.order_delivered_customer_date > o.order_estimated_delivery_date
order by delay_days desc
limit 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **12.	Find out which regions have the longest shipping times.**

-- COMMAND ----------

select c.customer_state, 
       avg(datediff(o.order_delivered_customer_date, o.order_purchase_timestamp)) as avg_shipping_time
from orders o
join customers c on o.customer_id = c.customer_id
where o.order_delivered_customer_date is not null
group by c.customer_state
order by avg_shipping_time desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##**5. Customer Reviews & Satisfaction**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **13.	Analyze the distribution of review scores.**

-- COMMAND ----------

DESCRIBE reviews;

-- COMMAND ----------

SELECT DISTINCT review_score
FROM reviews;

-- COMMAND ----------

SELECT DISTINCT review_score
FROM reviews;

-- COMMAND ----------

select cast(review_score as int) as review_score, count(*) as review_count, concat(round(count(*)/(
  select count(*) 
  from reviews 
  where review_score IN ('0','1', '2', '3', '4', '5'))*100,2), '%') 
  as percentage_distribution
from reviews
where review_score rlike '^[0-9]+$'  -- Filtering only numeric values
group by review_score
order by review_score desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **14.	Find the percentage of orders with low (1-2) vs. high (4-5) reviews.**

-- COMMAND ----------

--low vs high reviews
SELECT 
    CASE 
        WHEN review_score IN ('1', '2') THEN 'Low (1-2)' 
        WHEN review_score IN ('4', '5') THEN 'High (4-5)'
        ELSE 'Other' 
    END AS review_category,
    COUNT(*) AS review_count,
    (COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()) AS percentage
FROM reviews
WHERE review_score IN ('1', '2', '4', '5')
GROUP BY review_category
ORDER BY percentage DESC;

-- COMMAND ----------

--When we check even for mid reviews
SELECT 
    CASE 
        WHEN review_score IN ('1', '2') THEN 'Low (1-2)'
        WHEN review_score IN ('3') THEN 'Mid (3)'
        WHEN review_score IN ('4', '5') THEN 'High (4-5)'
        ELSE 'Other' 
    END AS review_category,
    COUNT(*) AS review_count,
    (COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()) AS percentage
FROM reviews
WHERE review_score IN ('1', '2', '3','4', '5')
GROUP BY review_category
ORDER BY percentage DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **15.	Identify the most common complaints in low-rated reviews.**

-- COMMAND ----------

--Most commom words
select 
    word, 
    count(*) as frequency
from (
    select explode(split(lower(review_comment_message), ' ')) as word
    from reviews
    where review_score in ('1', '2') 
        and review_comment_message is not null
) word_counts
where length(word) > 3 -- Filters out short/common words
group by word
order by frequency desc
limit 10;

-- COMMAND ----------

--Counting the phrases as is
select 
    review_comment_message, 
    count(*) as frequency
from reviews
where review_score in ('1', '2') 
    and review_comment_message is not null
group by review_comment_message
order by frequency desc
limit 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##**6. Time-Series & Seasonal Trends**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **16.	Find the month with the highest sales in 2018.**

-- COMMAND ----------

select 
    date_format(order_purchase_timestamp, 'yyyy-MM') as month, 
    sum(payment_value) as total_sales
from orders o
join payments p on o.order_id = p.order_id
where year(order_purchase_timestamp) = 2018
group by month
order by total_sales desc
limit 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **17.	Identify seasonal trends in customer purchases.**

-- COMMAND ----------

select 
    case 
        when month(order_purchase_timestamp) in (12, 1, 2) then 'Winter'
        when month(order_purchase_timestamp) in (3, 4, 5) then 'Spring'
        when month(order_purchase_timestamp) in (6, 7, 8) then 'Summer'
        when month(order_purchase_timestamp) in (9, 10, 11) then 'Fall'
    end as season,
    count(*) as total_orders,
    sum(payment_value) as total_sales
from orders o
join payments p on o.order_id = p.order_id
group by season
order by total_orders desc;

-- COMMAND ----------

select 
    case 
        when month(order_purchase_timestamp) in (12, 1, 2) then 'Winter'
        when month(order_purchase_timestamp) in (3, 4, 5) then 'Spring'
        when month(order_purchase_timestamp) in (6, 7, 8) then 'Summer'
        when month(order_purchase_timestamp) in (9, 10, 11) then 'Fall'
    end as season,
    month(order_purchase_timestamp) as month,
    count(order_id) as total_orders
from orders
group by season, month
order by month;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **18.	Determine whether higher review scores correlate with faster delivery times.**

-- COMMAND ----------

select 
    review_score,
    round(avg(datediff(order_delivered_customer_date, order_purchase_timestamp)), 2) as avg_delivery_days
from orders o
join reviews r on o.order_id = r.order_id
where review_score rlike '^[1-5]$' -- Ensure valid scores only
group by review_score
order by review_score;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##**7. Fraud Detection & Business Insights**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **19.	Identify orders with suspiciously high transaction values.**

-- COMMAND ----------

with payment_stats as (
    select avg(payment_value) as avg_payment, stddev(payment_value) as std_dev
    from payments
)
select p.order_id, p.payment_value
from payments p
join payment_stats s
on p.payment_value > (s.avg_payment + 3 * s.std_dev)  -- Find outliers
order by p.payment_value desc;

-- COMMAND ----------

select 
    order_id, 
    sum(price + freight_value) as total_transaction_value
from order_items
group by order_id
having total_transaction_value > (
    select avg(price + freight_value) + 3 * stddev(price + freight_value) from order_items
)
order by total_transaction_value desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **20.	Recommend strategies to improve customer retention & shipping efficiency.**
-- MAGIC

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### **Customer Retention Strategies**  
-- MAGIC 1. **Loyalty & Rewards Program**  
-- MAGIC    - Offer **discounts, cashback, or reward points** for repeat purchases.  
-- MAGIC    - Introduce **tier-based membership benefits** (e.g., free shipping for frequent buyers).  
-- MAGIC
-- MAGIC 2. **Personalized Marketing & Follow-Ups**  
-- MAGIC    - Use **email campaigns and SMS reminders** for abandoned carts.  
-- MAGIC    - Provide **custom product recommendations** based on past purchases.  
-- MAGIC
-- MAGIC 3. **Subscription & Bundling Offers**  
-- MAGIC    - Offer **monthly subscription plans** for repeat purchases (e.g., beauty or health products).  
-- MAGIC    - Bundle **related items** at a discount to encourage higher spending.  
-- MAGIC
-- MAGIC 4. **Improved Customer Support & Post-Purchase Engagement**  
-- MAGIC    - Send **follow-up emails with support/helpful guides** after a purchase.  
-- MAGIC    - Allow **easy returns & refunds** to build customer trust.  
-- MAGIC
-- MAGIC
-- MAGIC #### **Shipping Efficiency Strategies**  
-- MAGIC 1. **Optimize Delivery Partners & Warehouses**  
-- MAGIC    - Identify **worst-performing carriers** and **switch to reliable alternatives**.  
-- MAGIC    - Establish **regional fulfillment centers** to reduce shipping time in slow regions.  
-- MAGIC
-- MAGIC 2. **Offer Multiple Delivery Options**  
-- MAGIC    - Introduce **express delivery** for customers willing to pay extra.  
-- MAGIC    - Implement **same-day or next-day delivery** for high-order cities (e.g., São Paulo, Rio de Janeiro).  
-- MAGIC
-- MAGIC 3. **Improve Tracking & Customer Communication**  
-- MAGIC    - Provide **real-time tracking updates** to reduce complaints.  
-- MAGIC    - Notify customers of **delays proactively** and offer **compensation or discounts** for late deliveries.  
-- MAGIC
-- MAGIC 4. **Leverage Data for Route Optimization**  
-- MAGIC    - Analyze **historical shipping times** to optimize **carrier selection**.  
-- MAGIC    - Partner with **local delivery providers** for faster last-mile fulfillment.  
-- MAGIC  
-- MAGIC Combining **better customer engagement strategies** with **faster, more reliable shipping** will **increase retention and reduce churn**, ultimately boosting long-term revenue.
