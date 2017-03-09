-- Find the top 10 parts that with the highest quantity in returned orders. 
-- An order is returned if the returnflag field on any lineitem part is the character R.
-- Output schema: (part key, part name, quantity returned)
-- Order by: by quantity returned, descending.

-- Student SQL code here:

SELECT part.partkey, part.name, SUM(lineitem.quantity)
FROM lineitem 
INNER JOIN part ON lineitem.partkey = part.partkey
WHERE lineitem.returnflag = 'R'
GROUP BY lineitem.partkey
ORDER BY sum(lineitem.quantity) DESC
LIMIT 10;
