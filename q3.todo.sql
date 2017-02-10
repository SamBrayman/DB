with np as
(
	select nation.nationkey as key, nation.name as name, part.partkey as pkey, part.name as pname, sum(lineitem.quantity) as quantity
	from nation	
	inner join customer on nation.nationkey = customer.nationkey
        inner join orders on customer.custkey = orders.custkey
        inner join lineitem on orders.orderkey = lineitem.orderkey
        inner join part on lineitem.partkey = part.partkey
	group by nation.nationkey, lineitem.partkey, customer.nationkey, nation.nationkey, part.partkey--, orders.custkey, customer.nationkey
	order by nation.nationkey, sum(lineitem.quantity) desc
	)
select key, name, pkey, pname, max(quantity)
from np
group by key
;



