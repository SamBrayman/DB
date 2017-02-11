-- Find the top 10 parts that with the highest quantity in returned orders. 
-- An order is returned if the returnflag field on any lineitem part is the character R.
-- Output schema: (part key, part name, quantity returned)
-- Order by: by quantity returned, descending.

-- Student SQL code here:

select part.partkey, part.name, sum(lineitem.quantity)
from lineitem inner join part on lineitem.partkey = part.partkey
where lineitem.returnflag = 'R'
group by lineitem.partkey
order by sum(lineitem.quantity) desc
limit 10;
