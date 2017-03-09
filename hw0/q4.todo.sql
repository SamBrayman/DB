-- For each of the top 5 nations with the greatest value (i.e., total price) of orders placed,
-- find the top 5 nations which supply these orders.
-- Output schema: (Order placer name, Order supplier name, value of orders placed)
-- Order by: (Order placer, Order supplier)

-- Notes
--  1) We are expecting exactly 25 results 

-- Student SQL code here:

WITH go AS 
(
	SELECT nation.name AS order_nation_name, nation.nationkey AS order_nation_nationkey, SUM(orders.totalprice) AS order_nation_total_price, orders.orderkey AS order_keys
	FROM nation
	INNER JOIN customer ON nation.nationkey = customer.nationkey
	INNER JOIN orders ON customer.custkey = orders.custkey
	GROUP BY customer.nationkey
	ORDER BY order_nation_total_price DESC
	LIMIT 5
),

nation1 AS (
	SELECT order_nation_name AS order_nation_name1, order_nation_nationkey AS order_nation_nationkey
	FROM go
	LIMIT 1
),

nation2 AS (
	SELECT order_nation_name AS order_nation_name2, order_nation_nationkey AS order_nation_nationkey
        FROM go
        LIMIT 1 OFFSET 1
),

nation3 AS (
	SELECT order_nation_name AS order_nation_name3, order_nation_nationkey AS order_nation_nationkey
        FROM go
        LIMIT 1 OFFSET 2
),

nation4 AS (
	SELECT order_nation_name AS order_nation_name4, order_nation_nationkey AS order_nation_nationkey
        FROM go
        LIMIT 1 OFFSET 3
),

nation5 AS (
	SELECT order_nation_name AS order_nation_name5, order_nation_nationkey AS order_nation_nationkey
        FROM go
        LIMIT 1 OFFSET 4
),

all1 AS (
       	SELECT order_nation_name1 AS order_nation_name1, nation.name AS nation_names1, SUM(orders.totalprice) AS tot_price1
	FROM nation1, nation
	INNER JOIN customer ON order_nation_nationkey = customer.nationkey
        INNER JOIN orders ON customer.custkey = orders.custkey
        INNER JOIN lineitem ON orders.orderkey = lineitem.orderkey
        INNER JOIN supplier ON lineitem.suppkey = supplier.suppkey
        WHERE supplier.nationkey = nation.nationkey
        GROUP BY customer.nationkey, supplier.nationkey
        ORDER BY order_nation_name1, SUM(lineitem.extendedprice) DESC
	LIMIT 5
),

all2 AS (
        SELECT order_nation_name2 AS order_nation_name2, nation.name AS nation_names2, SUM(orders.totalprice) AS tot_price2
        FROM nation2, nation
        INNER JOIN customer on order_nation_nationkey = customer.nationkey
        INNER JOIN orders on customer.custkey = orders.custkey
        INNER JOIN lineitem on orders.orderkey = lineitem.orderkey
        INNER JOIN supplier on lineitem.suppkey = supplier.suppkey
        WHERE supplier.nationkey = nation.nationkey
        GROUP BY customer.nationkey, supplier.nationkey
        ORDER BY order_nation_name2, sum(lineitem.extendedprice) desc
        LIMIT 5
),

all3 AS (
        SELECT order_nation_name3 AS order_nation_name3, nation.name AS nation_names3, SUM(orders.totalprice) AS tot_price3
        FROM nation3, nation
        INNER JOIN customer ON order_nation_nationkey = customer.nationkey
        INNER JOIN orders ON customer.custkey = orders.custkey
        INNER JOIN lineitem ON orders.orderkey = lineitem.orderkey
        INNER JOIN supplier ON lineitem.suppkey = supplier.suppkey
        WHERE supplier.nationkey = nation.nationkey
        GROUP BY customer.nationkey, supplier.nationkey
        ORDER BY order_nation_name3, SUM(lineitem.extendedprice) DESC
        LIMIT 5
),

all4 AS (
        SELECT order_nation_name4 AS order_nation_name4, nation.name AS nation_names4, SUM(orders.totalprice) AS tot_price4
        FROM nation4, nation
        INNER JOIN customer ON order_nation_nationkey = customer.nationkey
        INNER JOIN orders ON customer.custkey = orders.custkey
        INNER JOIN lineitem ON orders.orderkey = lineitem.orderkey
        INNER JOIN supplier ON lineitem.suppkey = supplier.suppkey
        WHERE supplier.nationkey = nation.nationkey
        GROUP BY customer.nationkey, supplier.nationkey
        ORDER BY order_nation_name4, SUM(lineitem.extendedprice) DESC
        LIMIT 5
),

all5 AS (
        SELECT order_nation_name5 AS order_nation_name5, nation.name AS nation_names5, SUM(orders.totalprice) AS tot_price5
        FROM nation5, nation
        INNER JOIN customer ON order_nation_nationkey = customer.nationkey
        INNER JOIN orders ON customer.custkey = orders.custkey
        INNER JOIN lineitem ON orders.orderkey = lineitem.orderkey
        INNER JOIN supplier ON lineitem.suppkey = supplier.suppkey
        WHERE supplier.nationkey = nation.nationkey
        GROUP BY customer.nationkey, supplier.nationkey
        ORDER BY order_nation_name5, SUM(lineitem.extendedprice) DESC
        LIMIT 5
)


SELECT order_nation_name1, nation_names1, tot_price1
FROM all1
UNION
SELECT order_nation_name2, nation_names2, tot_price2
FROM all2
UNION
SELECT order_nation_name3, nation_names3, tot_price3
FROM all3
UNION
SELECT order_nation_name4, nation_names4, tot_price4
FROM all4
UNION
SELECT order_nation_name5, nation_names5, tot_price5
FROM all5
;
