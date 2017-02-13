-- For each of the top 5 nations with the greatest value (i.e., total price) of orders placed,
-- find the top 5 nations which supply these orders.
-- Output schema: (Order placer name, Order supplier name, value of orders placed)
-- Order by: (Order placer, Order supplier)

-- Notes
--  1) We are expecting exactly 25 results 

-- Student SQL code here:

with go as (
	select nation.name as order_nation_name, nation.nationkey as order_nation_nationkey, sum(orders.totalprice) as order_nation_total_price, orders.orderkey as order_keys
	from nation
	inner join customer on nation.nationkey = customer.nationkey
	inner join orders on customer.custkey = orders.custkey
	group by customer.nationkey-- nation.name--, orders.
	order by order_nation_total_price desc
	limit 5
)--,

--go2 as (
	select order_nation_name as order_nation_name, supplier.nationkey as supp_nation_key, sum(lineitem.extendedprice) as tot_price
	from go--, nation where supplier.nationkey = nation.nationkey
	inner join customer on order_nation_nationkey = customer.nationkey
	inner join orders on customer.custkey = orders.custkey   --have order from customers in different nations
	inner join lineitem on orders.orderkey = lineitem.orderkey --have lineitems from orders placed by top5 nations
	inner join supplier on lineitem.suppkey = supplier.suppkey -- have suppliers supplying all orders to top5 nations
	--inner join nation on supplier.nationkey = nation.name
	group by customer.nationkey, supplier.nationkey
	order by order_nation_name, sum(lineitem.extendedprice) desc
--) 


--select order_nation_name, nation.name, tot_price
--from nation, go2
--where nation.nationkey = supp_nation_key
--select 

;



--select order_nation_name
--from order5;

--uncomment above two statments to get top 5 order nations

/*
, order5Orders as (
	
	select order_nation_name as order_nation_name, orders.orderkey as order5_order_keys
	from orders
	inner join go on order_keys = orders.orderkey
	inner join customer on orders.custkey = customer.custkey
	inner join nation on customer.nationkey = nation.nationkey
	group by customer.nationkey
) 

--, lineItems as (
	
	select order_nation_name, nation.name, sum(lineitem.extendedprice)
	from order5Orders
	inner join lineitem on order5_order_keys = lineitem.orderkey
	inner join supplier on lineitem.suppkey = supplier.suppkey
	inner join nation on supplier.nationkey = nation.nationkey
	group by supplier.nationkey 
	order by sum(lineitem.extendedprice)
	
--), 

;





/*
, top5Nations as (
	select order_nation_name as order_nation_name, order_nation_nationkey as order_nation_nationkey
	from go
	order by order_nation_total_price desc
	limit 5
) --this select statement gives the top 5 order nations
-- want to select top 5 suppliers to these top 5 order nations

	
	--select order_nation_name, nation.name, sum(orders.totalprice)
	--select order_nation_name, nation.name
	select order_nation_name, nation.name, sum(orders.totalprice)
	from top5Nations
	inner join customer on customer.nationkey = order_nation_nationkey
	inner join orders on orders.custkey = customer.custkey
	inner join lineitem on orders.orderkey = lineitem.orderkey
	inner join supplier on lineitem.suppkey = supplier.suppkey
	inner join nation on supplier.nationkey = nation.nationkey
	group by orders.custkey, customer.nationkey--, supplier.nationkey
	--group by orders.custkey, customer.nationkey, lineitem.suppkey, supplier.nationkey
	
	
	
	--order by order_nation_name


;


/*
, top5_orders as (
	select orders *
	from top5Nations
	inner join orders on orders.custkey = cust_key
	inner join customer 
	group by cust

)

, all_orders as (
	
	select 
	from supplier
	inner join lineitem on supplier.suppkey = lineitem.suppkey
	inner join orders on lineitem.orderkey = orders.orderkey
	group by lineitem.suppkey, supplier.nationkey, orders.orderkey

)










/*


	select order_nation_name as order_name, nation.name as supp_name, sum(orders.totalprice) as tot_price
	from nation
	inner join customer on customer.nationkey = order_nation_name   --join in customers from a nation that has most orders
	inner join orders on orders.custkey = customer.custkey and 
	inner join lineitem on lineitem.orderkey = orders.orderkey
	group by orders.orderkey, 

*/
--from top5Nations on order_nation_nationkey =  

/*
, all_orders as (
	select nation.name as supplier_nation_name, sum(orders.totalprice) as tot_price
	from orders
	inner join nation on nation.name = order_nation_name 
	--inner join customers on orders.custkey = customer.custkey
	--inner join top5Nations on customer.nationkey = order_nation_nationkey
	--group by customer.nationkey
	
	inner join lineitem on lineitem.orderkey = orders.orderkey
	inner join supplier on supplier.suppkey = lineitem.suppkey
	group by supplier.nationkey, orders.orderkey
	order by sum(orders.totalprice) desc

) --this should give me all top5Nation orders grouped by nations


select order_nation_name, supplier_nation_name tot_price
from all_orders
inner join top5Nations
inner join go
-- order by order_nation_total_price desc, tot_price  desc;

;




*/

--with suppliers as (
--	select nation.name as supplier_nation_name, sum(orders.totalprice) 
--	from nation
--	inner join supplier on supplier.nationkey = 



--	inner join supplier on nation.nationkey = supplier.nationkey
	--inner join lineitem on supplier.suppkey = lineitem.suppkey
	--inner join orders on lineitem.orderkey = orders.orderkey
--	group by lineitem.suppkey, supplier.nationkey, orders.orderkey

--select order_nation_name, nation.name as supplier_nation_name, sum(orders.totalprice) 
--from orders
--inner join top5Nations on nation.







