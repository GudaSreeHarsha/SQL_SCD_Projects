DROP TABLE IF EXISTS retail_sales;
create table retail_sales(
	transaction_id int PRIMARY KEY,
	sale_date DATE,
	sale_time TIME  ,
	customer_id int,
	gender varchar(10),
	age int,
	category varchar(50),
	quantity int,
	price_per_unit FLOAT,
	cogs FLOAT,
	total_sale FLOAT
)

select * from retail_sales
limit 10

select count(*) from retail_sales


---- data cleaning
select *
from retail_sales
where transaction_id is null
or sale_date is null
or sale_time is null
or gender is null
or category is null
or quantity is null
or price_per_unit is null
or cogs is null
or total_sale is null

----
delete from retail_sales
where transaction_id is null
or sale_date is null
or sale_time is null
or gender is null
or category is null
or quantity is null
or price_per_unit is null
or cogs is null
or total_sale is null

---- data exploration
select * from retail_sales

--Count of sales

select count(1) as total_sales
from retail_sales

--Total no of customers

select count(Customer_id) as customer_id
from retail_sales 
group by customer_id
order by customer_id

--unique customers

select count (distinct Customer_id) as unique_customers
from retail_sales 

-- Total categories

select count(distinct category) count_of_categories 
from retail_sales

--category names

select category 
from retail_sales
group by category

----Business problems and solutions
--retrive all the columns for sales made on '2022-11-05'

select * 
from retail_sales
where sale_date ='2022-11-05'

--retrive all the transactions where the category is 'clothing' and the quantity sold is more than 10 in the month of november 2022

SELECT *
FROM retail_sales
WHERE category = 'Clothing'
  AND sale_date >= '2022-11-01'
  AND sale_date < '2022-12-01'
  and quantity >=4

--retrive the total sales for each category

select * from retail_sales

select category, sum(total_sale), count(*) as total_orders
from retail_sales
group by category

--find the average age of the customers 
  
select round(avg(age),2) average_age
from retail_sales
where category='Beauty'

--find all the transactions where total_sale is grater than 100

select * from retail_sales

select transaction_id, total_sale
from retail_sales
where total_sale>1000

--find the total number of transactions made by each gender in each category

select  gender, category, count(transaction_id)
from retail_sales
group by gender,category
order by category

--Calculate the average sale for each month and find out best selling month in each year

select * 
from
(
	select
	EXTRACT(year from sale_date) as year, 
	extract(month from sale_date) as month,
	avg(total_sale) as total_sale,
	dense_rank() over(partition by EXTRACT(year from sale_date) order by avg(total_sale)desc) as rank
	from retail_sales
	group by year, month
	order by year, total_sale desc
) as t1
where rank =1

--find top 5 customers based on highest sales

select * from retail_sales

--using limit

select customer_id, sum(total_sale) as total_sales
from retail_sales
group by customer_id
order by total_sales desc
limit 5

--using cte and rank
with cte as(
select customer_id, sum(total_sale) as total_sales,
rank() over (order by sum(total_sale) desc) as drnk
from retail_sales
group by customer_id
order by total_sales desc
)
select customer_id, total_sales
from cte
where drnk<=5



--Using cte and dense_rank

with ranked_sales as
(
	select customer_id, sum(total_sale) as total_sales,
	dense_rank() over (order by sum(total_sale) desc) as drnk
	from retail_sales
	group by customer_id
)
select customer_id, total_sales
from ranked_sales
where drnk<=5
order by total_sales desc

----Find the number of unique customers who purchased items from each category

select * from retail_sales

select 
	category,
	count(distinct customer_id) as count_unique_customers
from 
	retail_sales
group by 
	category

----create each shift and number of orders(example morning <12:00 noon, afternoon between 12:00 and 17:00 and evening >17:00)

select * from retail_sales 

with cte_shift
as
(
	select *,
	case
		when extract(hour from sale_time)<=12 then 'Morning'
		when extract(hour from sale_time) between 12 and 16 then 'Afternoon'
		else 'Evening'
		end
		as shift
	from retail_sales
)
select 
shift,
count(*) as total_orders_per_shift 
from cte_shift
group by shift


	








