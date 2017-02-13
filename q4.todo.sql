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
	group by customer.nationkey
	order by order_nation_total_price desc
	limit 5
),

nation1 as (
	select order_nation_name as order_nation_name1, order_nation_nationkey as order_nation_nationkey
	from go
	limit 1
),

nation2 as (
	select order_nation_name as order_nation_name2, order_nation_nationkey as order_nation_nationkey
        from go
        limit 1 offset 1
),

nation3 as (
	select order_nation_name as order_nation_name3, order_nation_nationkey as order_nation_nationkey
        from go
        limit 1 offset 2
),

nation4 as (
	select order_nation_name as order_nation_name4, order_nation_nationkey as order_nation_nationkey
        from go
        limit 1 offset 3
),

nation5 as (
	select order_nation_name as order_nation_name5, order_nation_nationkey as order_nation_nationkey
        from go
        limit 1 offset 4
),

all1 as (
       	select order_nation_name1 as order_nation_name1, nation.name as nation_names1, sum(orders.totalprice) as tot_price1
	from nation1, nation
	inner join customer on order_nation_nationkey = customer.nationkey
        inner join orders on customer.custkey = orders.custkey
        inner join lineitem on orders.orderkey = lineitem.orderkey
        inner join supplier on lineitem.suppkey = supplier.suppkey
        where supplier.nationkey = nation.nationkey
        group by customer.nationkey, supplier.nationkey
        order by order_nation_name1, sum(lineitem.extendedprice) desc
	limit 5
),

all2 as (
        select order_nation_name2 as order_nation_name2, nation.name as nation_names2, sum(orders.totalprice) as tot_price2
        from nation2, nation
        inner join customer on order_nation_nationkey = customer.nationkey
        inner join orders on customer.custkey = orders.custkey
        inner join lineitem on orders.orderkey = lineitem.orderkey
        inner join supplier on lineitem.suppkey = supplier.suppkey
        where supplier.nationkey = nation.nationkey
        group by customer.nationkey, supplier.nationkey
        order by order_nation_name2, sum(lineitem.extendedprice) desc
        limit 5
),

all3 as (
        select order_nation_name3 as order_nation_name3, nation.name as nation_names3, sum(orders.totalprice) as tot_price3
        from nation3, nation
        inner join customer on order_nation_nationkey = customer.nationkey
        inner join orders on customer.custkey = orders.custkey
        inner join lineitem on orders.orderkey = lineitem.orderkey
        inner join supplier on lineitem.suppkey = supplier.suppkey
        where supplier.nationkey = nation.nationkey
        group by customer.nationkey, supplier.nationkey
        order by order_nation_name3, sum(lineitem.extendedprice) desc
        limit 5
),

all4 as (
        select order_nation_name4 as order_nation_name4, nation.name as nation_names4, sum(orders.totalprice) as tot_price4
        from nation4, nation
        inner join customer on order_nation_nationkey = customer.nationkey
        inner join orders on customer.custkey = orders.custkey
        inner join lineitem on orders.orderkey = lineitem.orderkey
        inner join supplier on lineitem.suppkey = supplier.suppkey
        where supplier.nationkey = nation.nationkey
        group by customer.nationkey, supplier.nationkey
        order by order_nation_name4, sum(lineitem.extendedprice) desc
        limit 5
),

all5 as (
        select order_nation_name5 as order_nation_name5, nation.name as nation_names5, sum(orders.totalprice) as tot_price5
        from nation5, nation
        inner join customer on order_nation_nationkey = customer.nationkey
        inner join orders on customer.custkey = orders.custkey
        inner join lineitem on orders.orderkey = lineitem.orderkey
        inner join supplier on lineitem.suppkey = supplier.suppkey
        where supplier.nationkey = nation.nationkey
        group by customer.nationkey, supplier.nationkey
        order by order_nation_name5, sum(lineitem.extendedprice) desc
        limit 5
)


select order_nation_name1, nation_names1, tot_price1
from all1
UNION
select order_nation_name2, nation_names2, tot_price2
from all2
UNION
select order_nation_name3, nation_names3, tot_price3
from all3
UNION
select order_nation_name4, nation_names4, tot_price4
from all4
UNION
select order_nation_name5, nation_names5, tot_price5
from all5
;
