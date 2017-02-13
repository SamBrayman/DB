WITH T AS (
  SELECT
    nation.nationkey as placer_nationkey, 
    nation.name as placer_nationname,
    supplier.suppkey as suppkey,
    supplier.nationkey as supp_nation,
    orders.orderkey as orderkey,
    orders.totalprice as value
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
    
)
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
;
