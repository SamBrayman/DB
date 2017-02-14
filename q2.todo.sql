--  Find the 10 customers who spent the highest average number of days waiting for shipments.
-- A customer is waiting between a shipment's ship date and receipt date
-- Output schema: (customer key, customer name, average wait)
-- Order by: average wait DESC

-- Notes
--  1) Use the sqlite DATE(<text>) function to interpret a text field as a date.
--  2) Use subtraction to compute the duration between two dates (e.g., DATE(column1) - DATE(column2)).
--  3) Assume that a package cannot be received before it is shipped.

-- Student SQL code here:

SELECT customer.custkey, customer.name, AVG(DATE(lineitem.receiptdate) - DATE(lineitem.shipdate))
FROM customer
INNER JOIN orders ON customer.custkey = orders.custkey
INNER JOIN lineitem ON orders.orderkey = lineitem.orderkey
GROUP BY orders.custkey
ORDER BY AVG(DATE(lineitem.receiptdate) - DATE(lineitem.shipdate)) DESC
LIMIT 10;
