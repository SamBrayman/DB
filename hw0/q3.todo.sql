WITH np AS
(
	SELECT nation.nationkey AS key, nation.name AS name, part.partkey AS pkey, part.name AS pname, SUM(lineitem.quantity) AS quantity
	FROM nation	
	INNER JOIN customer ON nation.nationkey = customer.nationkey
        INNER JOIN orders ON customer.custkey = orders.custkey
        INNER JOIN lineitem ON orders.orderkey = lineitem.orderkey
        INNER JOIN part ON lineitem.partkey = part.partkey
	GROUP BY nation.nationkey, lineitem.partkey, customer.nationkey, nation.nationkey, part.partkey
	ORDER BY nation.nationkey, SUM(lineitem.quantity) DESC
)
SELECT key, name, pkey, pname, MAX(quantity)
FROM np
GROUP BY key
;



