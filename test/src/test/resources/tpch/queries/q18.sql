SELECT
    customer.c_name AS c_name,
    customer.c_custkey AS c_custkey,
    orders.o_orderkey AS o_orderkey,
    orders.o_orderdate AS o_orderdate,
    orders.o_totalprice AS o_totalprice,
    SUM( lineitem.l_quantity )
FROM
    customer
JOIN orders ON
    customer.c_custkey = orders.o_custkey
JOIN lineitem ON
    orders.o_orderkey = lineitem.l_orderkey
WHERE
    orders.o_orderkey IN(
        SELECT
            l_orderkey
        FROM
            lineitem
        GROUP BY
            l_orderkey
        HAVING
            SUM( l_quantity )> 300
    )
GROUP BY
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
ORDER BY
    o_totalprice DESC,
    o_orderdate LIMIT 100;
