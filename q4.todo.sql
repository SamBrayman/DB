-- For each of the top 5 nations with the greatest value (i.e., total price) of orders placed,
-- find the top 5 nations which supply these orders.
-- Output schema: (Order placer name, Order supplier name, value of orders placed)
-- Order by: (Order placer, Order supplier)

-- Notes
--  1) We are expecting exactly 25 results 

-- Student SQL code here:

with greatestOrderValueNations as (
	select nation.name as order_nation_name, sum(orders.totalprice) as order_nation_total_price
	from nation
	inner join customer on nation.nationkey = customer.nationkey
	inner join orders on customer.custkey = orders.custkey
	group by nation.name--, orders.
)
--with top5Nations as (
	select *
	from greatestOrderValueNations
	order by order_nation_total_price desc
	limit 5
--)

--	select order_nation_name, supplier_nation_name, 






;
