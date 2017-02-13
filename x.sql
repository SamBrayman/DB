WITH T AS (
  SELECT
    nation.nationkey as placer_nationkey,
    nation.name as placer_nationname,
    supplier.suppkey as suppkey,
    supplier.nationkey as supp_nation,
    orders.orderkey as orderkey,
    lineitem.extendedprice as value
  FROM
    nation,
    customer,
    orders,
    lineitem,
    supplier
  WHERE
    customer.nationkey = nation.nationkey
    AND orders.custkey = customer.custkey
    AND lineitem.orderkey = orders.orderkey
    AND supplier.suppkey = lineitem.suppkey

),
t1 as
(
  SELECT
  placer_nationname, nation.name as supplier_nationname, sum(T1.value) as value
FROM
  T as T1,
  (SELECT placer_nationkey, sum(value) as value
   FROM T
   GROUP BY placer_nationkey
   ORDER BY value desc
   LIMIT 5
  ) as T2,
  nation
WHERE
  nation.nationkey = T1.supp_nation
  AND T1.placer_nationkey = T2.placer_nationkey
GROUP BY placer_nationname, supplier_nationname
ORDER BY placer_nationname, supplier_nationname
)
SELECT a.placer_nationname, a.supplier_nationname, a.value
FROM t1 AS a
WHERE a.value >= (SELECT MIN(b.value)
                FROM t1 AS b
                WHERE (SELECT COUNT(*)
                       FROM t1 AS c
                       WHERE c.placer_nationname = b.placer_nationname AND c.value >= b.value) <= 5
                GROUP BY b.placer_nationname)
ORDER BY a.placer_nationname, a.value DESC
