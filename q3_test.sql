with np as
(
	select nation.nationkey, nation.name, part.partkey, part.name, sum(lineitem.quantity)
	from nation	
	inner join customer on nation.nationkey = customer.nationkey
        inner join orders on customer.custkey = orders.custkey
        inner join lineitem on orders.orderkey = lineitem.orderkey
        inner join part on lineitem.partkey = part.partkey
	group by lineitem.partkey, customer.nationkey, nation.nationkey, part.partkey--, orders.custkey, customer.nationkey
	--where sum(lineitem.quantity) = max(lineitem.quantity)
	order by nation.nationkey, sum(lineitem.quantity) desc
	--limit 2
)
select *
from np
;



