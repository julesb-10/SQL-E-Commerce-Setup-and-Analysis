-- Data Source: https://www.kaggle.com/datasets/cemeraan/fecom-inc-e-com-marketplace-orders-data-crm

-- PART 1: IMPORTING DATA + DATA CLEANING + DEFINING RELATIONSHIPS
-- ADD QUERIES CREATING TABLES HERE

-- NOTE: Tables were created directly using Postgresql's Create Table but would be simple to do using queries as well

-- Before defining relationships, need to fix the latitude and longitude columns in the geolocations table:
-- Replacing commas with periods for formatting:
update geolocations
	set geo_lat = replace(geo_lat, ',', '.'),
		geo_lon = replace(geo_lon, ',', '.');

-- Converting values to numeric:
alter table geolocations
alter column geo_lat type numeric using geo_lat::numeric,
alter column geo_lon type numeric using geo_lon::numeric;


--Geolocations also has an abundance of null rows, getting rid of these:
delete from geolocations
where coalesce(
  geo_postal_code::text,
  geo_lat::text,
  geo_lon::text,
  geolocation_city::text,
  geo_country::text
) is null;


--Starting with customer_list table:
select * from customer_list;

-- subscriber_id is supposed to be a unique identifier, checking for duplicates:

select subscriber_id, count(*) as count
from customer_list
group by subscriber_id
having count(*) > 1;

-- There are many duplicates as a result of the customer_trx_id, or more simply put,
-- customers that have made multiple purchases

-- I will make a separate customer table with the unique entries for subscriber_id to 
-- then be able to define relationships between tables

--Creating copy with unique subscriber_id entries, as well as defining the primary key:
create table customer_list_clean as
select distinct on (subscriber_id)
    customer_trx_id,
    subscriber_id,
    subscribe_date,
    first_order_date,
    customer_postal_code,
    customer_city,
    customer_country,
    customer_country_code,
    age,
    gender
from customer_list
where customer_trx_id is not null
order by subscriber_id, subscribe_date;


--Making sure there aren't duplicates anymore in subscriber_id column:
select subscriber_id, count(*) from customer_list_clean
	group by subscriber_id
	having count(*) > 1;
--Good

--Defining subscriber_id as primary key:
alter table customer_list_clean
	add primary key (subscriber_id);
		
	
-- adding unique constraint to orders table for customer_trx_id for the following relationship to execute without error:
alter table orders
add constraint uq_customer_trx_id unique (customer_trx_id);

alter table customer_list_clean
add constraint fk_customer_order
foreign key (customer_trx_id)
references orders(customer_trx_id);	

-- RELATIONSHIP DEFINED: customer_list_clean.customer_trx_id = orders.customer_trx_id
	
-- relationship between customer_list_clean.customer_postal_code and geolocations.geo_postal_code
-- to do this, we will impose a unique constraint on the combinations of postal codes and cities as
-- some postal codes correspond to 2 cities:
alter table geolocations
add constraint geolocations_postal_city_unique
unique (geo_postal_code, geolocation_city);


-- relationship (composite key) customer_list_clean.customer_postal_code = geolocations.geo_postal_code (many to 1), as 
alter table customer_list_clean
add constraint fk_customer_postal_city
foreign key (customer_postal_code, customer_city)
references geolocations (geo_postal_code, geolocation_city);

alter table customer_list_clean
add constraint fk_postal_city
foreign key (customer_postal_code, customer_city)
references geolocations (geo_postal_code, geolocation_city);


-- Relationship between geolocations and sellers_list (via postal code and city):
-- First defining seller_id as primary key (could've done during import but for the sake of demonstration, doing it manually)
alter table sellers_list
	add constraint sellers_list_pkey primary key (seller_id);


-- Note on sellers_list: There appear to be many duplicate entries with all identical values (postal code, city, etc.)
-- In a real world situation I would try to find out if this corresponds to many branches in the same city which are uniquely identified,
-- and act accordingly but I dont have this luxury here.

-- I was going to make a cleaned table with duplicate postal code values per seller removed, however this would affect
-- the order_items table, as there are 3095 unique seller_ids there, same as the sellers table. as a result, we'll keep the table as is

-- sellers_list relationships with other tables:
-- Defining a composite foreign key constraint as in the geolocations table

alter table sellers_list
add constraint fk_seller_location
foreign key (seller_postal_code, seller_city)
references geolocations (geo_postal_code, geolocation_city);


-- sellers_list and order_items:
alter table order_items
	add constraint order_items_sellid_fkey
	foreign key (seller_id) references sellers_list (seller_id);


--orders table relationships: (with with order_reviews on order_id, with order_items on order_id, order_payments on order_id)
-- First defining order_id as unique and primary key
alter table orders
	add constraint unique_order_id unique (order_id);

alter table orders
	add primary key (order_id); 

alter table order_reviews
	add constraint reviews_ord_id_fkey
	foreign key (order_id) references orders (order_id);

alter table order_items
	add constraint ord_items_fkey
	foreign key (order_id) references orders (order_id);
	
alter table order_payments
	add constraint pmts_ord_id_fkey
	foreign key (order_id) references orders (order_id);
	
	
	
-- order_items relationships: (with products on product_id, order_reviews on order_id, orders on order_id)
-- first, defining product_id column in products to be primary key
--Checking for duplicates first:
select product_id, count(*) as duplicates from products
	group by product_id
	having count(*) > 1;
-- all good

alter table products
	add primary key (product_id);

alter table order_items
	add constraint ord_prod_id_fkey
	foreign key (product_id) references products(product_id);
	
-- INTER-TABLE RELATIONSHIPS DEFINED

-- PART 2: QUERIES ATTEMPTING TO ANSWER REAL-WORLD QUESTIONS THAT COULD ARISE

-- 1. Total number of customers
select count(*) as total_customers from customer_list_clean;
-- 96096

-- 2. List of unique cities from customer data
select distinct customer_city from customer_list_clean
order by customer_city;

-- 3. Total number of orders
select count(*) as "Total Orders" from orders;
-- 99441 Total orders

-- 4. Number of orders per order status
select order_status, count(*) as total_orders from orders
	group by order_status;

-- 5. Top 5 most frequent payment types
select payment_type, count(*) as total from order_payments
	group by payment_type
	order by total desc
	limit 5;
	
-- 6. Total revenue generated
select sum(payment_value) as total_revenue from order_payments;
-- 16,008,872.12

-- Per Month:
select date_trunc('month', order_purchase_timestamp) as month, sum(payment_value) as revenue from order_payments op
join orders o on op.order_id = o.order_id
group by month
order by month;


-- 7. Average review score
select avg(review_score) from order_reviews;
-- 4.09


-- 8. Number of products sold per product category
select product_category_name, count(*) as total_sold from products p
	inner join order_items ord
	on p.product_id = ord.product_id
	group by product_category_name
	order by total_sold desc;


-- 9. Top 10 sellers by number of orders
select seller_name, count(distinct o.order_id) as number_orders from sellers_list s
	inner join order_items o 
	on s.seller_id = o.seller_id
	group by s.seller_name
	order by number_orders desc
	limit 10;
-- NOTE: Very important to count DISTINCT order_id as order_items table can have multiple items per order,
-- hence affecting the calculation


-- 10. Number of customers per country
select customer_city, count(*) as "Number of Customers" from customer_list_clean
	group by customer_city
	order by "Number of Customers" desc;


-- 11. Monthly order count trend
select extract (year from order_purchase_timestamp) as year,
	extract (month from order_purchase_timestamp) as month,
	count(*) as orders_in_month from orders
		group by year, month
		order by year, month;
		
		
--Similarly, using date_trunc:
select date_trunc('month', order_purchase_timestamp) as month,
	count(*) as orders_in_month from orders
	group by month
	order by month;


-- 12. Average delivery time per product category (in days)
select p.product_category_name as product_category, avg(order_delivered_customer_date - order_purchase_timestamp) as average_delivery_time
from orders o
	inner join order_items oi on o.order_id = oi.order_id
	inner join products p on p.product_id = oi.product_id
	where o.order_status = 'delivered'
	group by product_category
	order by average_delivery_time;


--Note: in results, there is a category name of '#N/A', indicating some missing values in the products table for this column. Investigating:
select count(*) from products
where product_category_name = '#N/A';
-- 623 missing category names. Technically there's nothing I can do, in real life would try to contact the person necessary 
-- who has a product list that I could use to map and fill the values. for now, I'll just generate a list of seller names 
-- to which these products are associated that would need to be contacted for clarification (assuming they have the same product IDs)

select p.product_id, p.product_category_name, s.seller_name from products p
join order_items oi on p.product_id = oi.product_id
join sellers_list s on oi.seller_id = s.seller_id
where product_category_name = '#N/A';

--Could just take unique seller names from the above and contact them


-- 13. Top 5 products with highest average review score
select p.product_id, p.product_category_name, round(avg(review_score), 3) average_review from order_reviews orv
	inner join order_items oi on oi.order_id = orv.order_id
	inner join products p on oi.product_id = p.product_id
	group by p.product_id, p.product_category_name
	order by average_review desc
	limit 20;
	
	
-- Top 20 all have 5 star averages, so maybe better to just get the top 10 product categories to get a better general idea:
select p.product_category_name, round(avg(review_score), 3) average_review from order_reviews orv
	inner join order_items oi on oi.order_id = orv.order_id
	inner join products p on oi.product_id = p.product_id
	group by p.product_category_name
	order by average_review desc
	limit 10;
	
	
-- 14. Customers with more than 5 orders	(COME BACK TO)
select c.subscriber_id, count(*) as total_orders from orders o
	inner join customer_list c on o.customer_trx_id = c.customer_trx_id
	group by c.subscriber_id
	having count(*) > 5
	order by total_orders desc;
-- using original customer list table as without multiple instances of customer_trx_id, nobody will have more than 1 order


-- Alternatively (and more simply), the following query gives the same results
select subscriber_id, count(customer_trx_id) total_orders from customer_list
	group by subscriber_id
	having count(customer_trx_id) > 5
	order by total_orders desc


-- 15. Revenue per seller
select s.seller_id, sum(oi.price) as revenue from order_items oi
	inner join sellers_list s on
		oi.seller_id = s.seller_id
	group by s.seller_id
	order by revenue desc;


-- 16. Most common payment installment count
select payment_installments, count(*) as total from order_payments
	group by payment_installments
	order by total desc;
-- orders often paid for in full up front, however a large % of them are paid in more than 1 installment

-- 17. Orders with multiple sellers	
select order_id, count(distinct seller_id) as number_sellers from order_items
	group by order_id
	having count(distinct seller_id) > 1
	order by number_sellers desc;
	
	
-- 18. Average number of items per order
select avg(number_items) as mean_order_size from
	(select order_id, count(*) as number_items from order_items
	group by order_id);


-- 19. Top 5 cities by number of orders
select c.customer_city, count(o.order_id) number_of_orders from orders o
	inner join customer_list_clean c on c.customer_trx_id = o.customer_trx_id
	group by c.customer_city
	order by number_of_orders desc
	limit 5;


-- 20. Percentage of orders delivered late
select 
	round(100.0 * count(*) filter (where order_delivered_customer_date > order_estimated_delivery_date) / count(*), 2) 
								as late_delivery_percentage
	from orders
	where order_delivered_customer_date is not null;
-- could've also used: where order_status = 'delivered' to filter


-- 21. Monthly Revenue Trend:
select date_trunc('month', o.order_purchase_timestamp) as month,
	sum(op.payment_value) as revenue
from orders o
	inner join order_payments op on o.order_id = op.order_id
group by month
order by month;

-- Alternatively (using extract to get months in number format):
select extract('year' from o.order_purchase_timestamp) as year,
		extract('month' from o.order_purchase_timestamp) as month,
		sum(op.payment_value) as revenue
from orders o
	inner join order_payments op on o.order_id = op.order_id
group by year, month
order by year, month;


-- 22. Top 5 product categories by revenue
select p.product_category_name, to_char(sum(oi.price), 'FM999,999,999,990.00') revenue from products p
	inner join order_items oi on p.product_id = oi.product_id
	group by p.product_category_name
order by sum(oi.price) desc
limit 5;


-- 23. Number of reviews per review score
select review_score, count(*) as number_reviews 
from order_reviews
group by review_score
order by review_score;
-- Alarming amount of 1 star reviews

-- Seeing which sellers have lots of 1 star reviews:

with one_star_revs as (select * from order_reviews rev 
where review_score = 1)

select s.seller_id, count(*) as total_one_star_reviews from one_star_revs osr
join order_items oi on osr.order_id = oi.order_id
join sellers_list s on oi.seller_id = s.seller_id
group by s.seller_id
order by total_one_star_reviews desc;

-- With this information, a meassure could be put in place that warns sellers after a certain amount of bad reviews,
-- and eventually bans them after a certain thershold of bad reviews is reached
-- average review score could also be used, in which case the warning and ban would come after falling below a threshold

-- While we're here: Sellers with highest average review score
select s.seller_id, avg(review_score) avg_rating from order_reviews rev
join order_items oi on rev.order_id = oi.order_id
join sellers_list s on oi.seller_id = s.seller_id
group by s.seller_id
order by avg_rating desc;


-- 24. Average (seller) freight value per country
select s.seller_country, avg(freight_value) as mean_freight_value from order_items oi
	inner join sellers_list s on oi.seller_id = s.seller_id
	group by s.seller_country
	order by mean_freight_value desc;

-- doing the same for customers:
select c.customer_country country, avg(oi.freight_value) mean_freight_value from order_items oi
	join orders o on oi.order_id = o.order_id
	join customer_list_clean c on o.customer_trx_id = c.customer_trx_id
group by country
order by mean_freight_value desc;
	
	
-- 25. 	Top 5 customers by total spending (database doesn't have customer names so I can only output their IDs)
select c.customer_trx_id, sum(op.payment_value) total_spent from customer_list c
	join orders o on c.customer_trx_id = o.customer_trx_id
	join order_payments op on o.order_id = op.order_id
	group by c.customer_trx_id
	order by total_spent desc
	limit 5;
	
	
-- 26. Average number of installments per payment type
select payment_type, avg(payment_installments) as mean_number_installments
from order_payments
group by payment_type

-- results are pretty obvious (only payment type with a mean > 1 is credit cards)
-- Not necessarily useful in this case but using window functions for the sake of it: 

select *, round(avg(payment_installments) over(partition by payment_type), 3) as mean_number_installments
from order_payments;


-- 27. Top 10 orders with highest freight value
select order_id, sum(freight_value) as order_freight_value from order_items
	group by order_id
	order by order_freight_value desc
	limit 10;
	
	
-- 28. Customers who have never left a review
select c.subscriber_id
from customer_list_clean c
left join orders o on c.customer_trx_id = o.customer_trx_id
left join order_reviews r on o.order_id = r.order_id
where r.review_id is null;


-- 29. Average product price per category
select p.product_category_name, round(avg(oi.price), 2) average_price from products p
	inner join order_items oi on p.product_id = oi.product_id
	group by p.product_category_name
	order by average_price desc;
	
	
-- 30. Top 5 countries by total revenue 
select customer_country, sum(payment_value) as revenue from customer_list c
	join orders o on c.customer_trx_id = o.customer_trx_id
	join order_payments op on o.order_id = op.order_id
	group by customer_country
	order by revenue desc
	limit 10;
	
	
-- 31. Monthly customer acquisition trend
select extract('year' from first_order_date) as year,
		extract('month' from first_order_date) as month,
		count(*) as new_customers
from customer_list_clean c
group by year, month
order by year, month;

-- Alternatively, can format the years and months differently if desired:
select to_char(first_order_date, 'YYYY-MM') as year_month, count(*) as new_customers
from customer_list_clean
group by year_month
order by year_month;


-- 32. Classifying Recurrent vs One-Time Customers: 

with number_orders_by_customer as (
	select subscriber_id, count(*) as order_count from orders o
	join customer_list c on o.customer_trx_id = c.customer_trx_id
	group by subscriber_id
)
select subscriber_id, 
	case
		when order_count = 1 then 'One-Time'
		else 'Recurrent' end as customer_type
from number_orders_by_customer;

-- Further, if we simply want the number of recurrent and one-time customers respectively (using previous query as subquery):
select customer_type, count(*) as total
from (
	with number_orders_by_customer as (
	select subscriber_id, count(*) as order_count from orders o
	join customer_list c on o.customer_trx_id = c.customer_trx_id
	group by subscriber_id
)
	select subscriber_id, 
	case
		when order_count = 1 then 'One-Time'
		else 'Recurrent' end as customer_type
	from number_orders_by_customer
)
group by customer_type;




-- 33. Average delivery delay per seller 

-- The following gives average delays by seller and INCLUDES early deliveries in calculation (so that early deliveries will help)
select s.seller_id, avg(order_delivered_customer_date - order_estimated_delivery_date) as avg_delay from orders o
join order_items oi on o.order_id = oi.order_id
join sellers_list s on oi.seller_id = s.seller_id
where o.order_status = 'delivered'
group by s.seller_id
order by avg_delay desc;

-- If we want the average delay ONLY when the delivery is late (ie how late it is WHEN it is late):
select s.seller_id, avg(order_delivered_customer_date - order_estimated_delivery_date) as avg_delay from orders o
join order_items oi on o.order_id = oi.order_id
join sellers_list s on oi.seller_id = s.seller_id
where o.order_status = 'delivered' and (order_delivered_customer_date > order_estimated_delivery_date)
group by s.seller_id
order by avg_delay desc;

-- this information could then be used to maybe give sellers a score or give them a "fast shipping" tag on the website, amongst other things 


-- 35. Customer Lifetime Value
select subscriber_id, sum(payment_value) as lifetime_value from order_payments op
join orders o on op.order_id = o.order_id
join customer_list c on o.customer_trx_id = c.customer_trx_id
group by c.subscriber_id
order by lifetime_value desc;


-- 36. Monthly Average Order Value
select * from orders
select extract('year' from order_purchase_timestamp) as year,
	extract('month' from order_purchase_timestamp) as month,
	avg(op.payment_value) as average_order_value
from orders o
join order_payments op on o.order_id = op.order_id
group by year, month
order by year, month;


-- 37. Top 5 worst sellers by average review score
select s.seller_id, round(avg(review_score), 3) as avg_score from order_reviews rev
join order_items oi on rev.order_id = oi.order_id
join sellers_list s on oi.seller_id = s.seller_id
group by s.seller_id
order by avg_score asc
limit 5;

-- say we want to remove all sellers with an avg review score below 2.2 for example, we would ban the following:
select s.seller_id, round(avg(review_score), 3) as avg_score from order_reviews rev
join order_items oi on rev.order_id = oi.order_id
join sellers_list s on oi.seller_id = s.seller_id
group by s.seller_id
having round(avg(review_score), 3) < 2.2;


-- 38. Product categories with highest (avg) freight_value 
select p.product_category_name, avg(oi.freight_value) as mean_freight_value from order_items oi
join products p on oi.product_id = p.product_id
group by product_category_name
order by mean_freight_value desc;

-- Top 10 most profitable products (highest price - freight_value)
select distinct product_id, (price - freight_value) margin from order_items
order by margin desc
limit 10;


-- Top 10 most profitable product categories on average by margin (highest price - freight_value) 
with profit_margin as (select distinct product_id, (price - freight_value) margin from order_items)

select p.product_category_name, round(avg(margin), 2) as avg_margin from profit_margin pm
join products p on pm.product_id = p.product_id
group by p.product_category_name
order by avg_margin desc
limit 10;

-- Top 10 most profitable product categories by total profit:
with profit_margin as (select product_id, (price - freight_value) margin from order_items)

select p.product_category_name, round(sum(margin), 2) as profit from profit_margin pm
join products p on pm.product_id = p.product_id
group by p.product_category_name
order by profit desc
limit 10;


-- 39. Average time between order purchase and delivery per seller country
select * from orders
select s.seller_country, avg(order_delivered_customer_date - order_purchase_timestamp) avg_delivery_time from orders o
join order_items oi on o.order_id = oi.order_id
join sellers_list s on oi.seller_id = s.seller_id
group by s.seller_country
order by avg_delivery_time;


-- 40. Sellers with highest average review scores
select s.seller_id, avg(review_score) as avg_rating from order_reviews rev
join order_items oi on rev.order_id = oi.order_id
join sellers_list s on oi.seller_id = s.seller_id
group by s.seller_id
order by avg_rating desc;


-- 41. Monthly growth rate of orders (along with cumulative sum of orders)
with monthly_orders as (
	select date_trunc('month', order_purchase_timestamp) as month, count(*) as total_orders
	from orders
	group by month
	
)
select month, total_orders,
	round((total_orders - lag(total_orders) over (order by month)) * 100.0 / lag(total_orders) over (order by month), 2)
	as growth_rate,
	sum(total_orders) over(order by month) as cum_sum
from monthly_orders;

-- if you prefer year and month seperated with month as its number representation, here you go:

with monthly_orders as (
select extract ('year' from order_purchase_timestamp) as year,
	extract ('month' from order_purchase_timestamp) as month,
	count(*) as total_orders
	from orders
	group by year, month
)
select year, month, total_orders,
	round((total_orders - lag(total_orders) over(order by year, month)) * 100.0 / lag(total_orders) over(order by year, month), 2)
	as growth_rate,
	sum(total_orders) over(order by month) as cum_sum
from monthly_orders;


-- 42. Top 5 products by revenue
select p.product_id, p.product_category_name, sum(oi.price) as revenue from products p
join order_items oi on p.product_id = oi.product_id
group by p.product_id, p.product_category_name
order by revenue desc
limit 5;

--Additionally: displaying product IDs, their category, and their rank by revenue

select p.product_id, p.product_category_name, sum(oi.price) as revenue,
	rank() over(order by sum(oi.price) desc) as revenue_rank 
	from products p
	join order_items oi on p.product_id = oi.product_id
	group by p.product_id, p.product_category_name
	order by revenue_rank;
	
	
-- 43. Longest delivery delays per product category
select p.product_category_name, 
	max(order_delivered_customer_date - order_estimated_delivery_date) as max_delay 
	from orders o
join order_items oi on o.order_id = oi.order_id
join products p on oi.product_id = p.product_id
where o.order_status = 'delivered'
group by p.product_category_name
order by max_delay desc;


-- 44. Top 5 sellers by average monthly sales
with monthly_sales as(

	select oi.seller_id, date_trunc('month', order_purchase_timestamp) as month, 
	sum(oi.price) sales_for_month from orders o
	join order_items oi on o.order_id = oi.order_id
	group by oi.seller_id, month
)

select seller_id, round(avg(sales_for_month), 2) avg_monthly_sales 
from monthly_sales
group by seller_id
order by avg_monthly_sales desc;
	
	
-- 45. Orders where review was left before delivery (data quality check)

select o.order_id, review_score, review_comment_message_en from order_reviews rev
join orders o on rev.order_id = o.order_id
where review_creation_date < order_purchase_timestamp
and review_comment_message_en is not null;
-- 74 orders where this needs to be investigated


-- 46. Most popular product categories each month 
with cat_monthly_orders as(
	select date_trunc('month', order_purchase_timestamp) as month, p.product_category_name, count(*) as orders
	from orders o
	join order_items oi on o.order_id = oi.order_id
	join products p on oi.product_id = p.product_id
	where o.order_status = 'delivered'
	group by month, p.product_category_name
	
),
category_ranking as (
	select *, row_number() over(partition by month order by orders desc) as ranking
	from cat_monthly_orders
)

select month, product_category_name as most_popular_category, orders
from category_ranking
where ranking = 1
order by month;



-- 47. Percentage of orders delivered late by seller
select s.seller_id, 
count(*) filter (where order_delivered_customer_date > order_estimated_delivery_date ) * 100.0 / count(*) as percent_late
from orders o
join order_items oi on o.order_id = oi.order_id
join sellers_list s on oi.seller_id = s.seller_id
where o.order_delivered_customer_date is not null
group by s.seller_id
order by percent_late desc;


-- 48. Categories with highest average revenue per product
with product_category_revenue as (
	select p.product_category_name, sum(oi.price) as category_revenue, count (distinct p.product_id) as number_products_in_cat
	from order_items oi
	join products p on oi.product_id = p.product_id
	group by p.product_category_name
)

select product_category_name, round((category_revenue / nullif(number_products_in_cat, 0)), 2) as avg_revenue_per_product
from product_category_revenue
order by avg_revenue_per_product desc
limit 10;


-- 49. First and most recent order per customer
select c.subscriber_id,
min(order_purchase_timestamp) as first_order_date,
max(order_purchase_timestamp) as most_recent_order_date
from orders o
join customer_list c on o.customer_trx_id = c.customer_trx_id
group by c.subscriber_id;


-- 50. Running total of revenue per month
with monthly_revenue as (	
	select date_trunc('month', order_purchase_timestamp) as month, 
	sum(op.payment_value)  as revenue
	from orders o
	inner join order_payments op on o.order_id = op.order_id
	group by month
)

select *, sum(revenue) over(order by month) as cumulative_revenue
from monthly_revenue;


-- 51. Top 3 most expensive products in each category
with max_product_price as (
	select p.product_id,
		   p.product_category_name,
		   max(oi.price) as max_price
	from products p
	join order_items oi on p.product_id = oi.product_id
	group by p.product_id, p.product_category_name
)

select *
from (
  select *,
         row_number() over (partition by product_category_name order by max_price desc) as ranking
  from max_product_price
) ranked
where ranking <= 3;


-- 52. Month-over-month revenue growth rate per seller
with monthly_revenue as (
  select
    seller_id,
    date_trunc('month', order_purchase_timestamp) as month,
    sum(price) as revenue
  from orders o
  join order_items oi on o.order_id = oi.order_id
  where order_status = 'delivered'
  group by seller_id, month
)

select
  seller_id,
  month,
  revenue,
  round(
    (revenue - lag(revenue) over (partition by seller_id order by month)) * 100.0
    / nullif(lag(revenue) over (partition by seller_id order by month), 0), 2
  ) as revenue_growth_rate
from monthly_revenue
order by seller_id, month;
