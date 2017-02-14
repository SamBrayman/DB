-- Find the customer market segments where the yearly total number of orders declines
-- in the last 2 years in the dataset. Note the database will have different date
-- ranges per market segment, for example segment A records between 1990-1995, and
-- segment B between 1992-1998. That is for segment A, we want the difference between
-- 1995 and 1994.
-- Output schema: (market segment, last year for segment, difference in # orders)
-- Order by: market segment ASC

-- Notes
--  1) Use the sqlite function strftime('%Y', <text>) to extract the year from a text field representing a date.
--  2) Use CAST(<text> as INTEGER) to convert text (e.g., a year) into an INTEGER.
--  3) You may use a SQL 'WITH' clause.

-- Student SQL code here:


WITH go AS (
	SELECT customer.mktsegment AS c_m_s, CAST(strftime('%Y', orders.orderdate) AS INTEGER) AS year, COUNT(orders.orderkey) AS num_orders
  	FROM customer
    	INNER JOIN orders ON orders.custkey = customer.custkey
  	GROUP BY c_m_s, year
),

go2 AS (
	SELECT customer.mktsegment AS c_m_s, MAX(CAST(strftime('%Y', orders.orderdate) AS INTEGER)) AS last_year
	FROM customer
  	INNER JOIN orders ON customer.custkey = orders.custkey
  	GROUP BY c_m_s
)



SELECT go2.c_m_s, go2.last_year, table2.num_orders - table3.num_orders AS diff
FROM go2
INNER JOIN go AS table2 ON go2.c_m_s = table2.c_m_s AND go2.last_year = table2.year
INNER JOIN go AS table3 ON table2.year = table3.year + 1 AND table2.c_m_s = table3.c_m_s AND diff < 0
ORDER BY go2.c_m_s ASC;
