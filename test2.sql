 with go as (
          select nation.name as order_nation_name, nation.nationkey as order_nation_nationkey, sum(orders.totalprice) as order_nation_total_price
          from nation, customer, orders
          --inner join customer on nation.nationkey = customer.nationkey
          --inner join orders on customer.custkey = orders.custkey
         where nation.nationkey = customer.nationkey
       	 and customer.custkey = orders.custkey	
	 group by nation.name, customer.nationkey-- nation.name--, orders.
          order by order_nation_total_price desc
          limit 5
)

select *
from go;
