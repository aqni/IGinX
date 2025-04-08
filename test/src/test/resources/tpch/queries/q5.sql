SELECT
    nation.n_name AS n_name,
    SUM( lineitem.l_extendedprice *( 1 - lineitem.l_discount )) AS revenue
FROM
    customer
JOIN orders ON
    customer.c_custkey = orders.o_custkey
JOIN lineitem ON
    lineitem.l_orderkey = orders.o_orderkey
JOIN supplier ON
    lineitem.l_suppkey = supplier.s_suppkey
    AND customer.c_nationkey = supplier.s_nationkey
JOIN nation ON
    supplier.s_nationkey = nation.n_nationkey
JOIN region ON
    nation.n_regionkey = region.r_regionkey
WHERE
    region.r_name = "ASIA"
    AND orders.o_orderdate >= 757353600000
    AND orders.o_orderdate < 788889600000
GROUP BY
    n_name
ORDER BY
    revenue DESC;
