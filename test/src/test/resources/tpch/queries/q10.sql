SELECT
    customer.c_custkey AS c_custkey,
    customer.c_name AS c_name,
    SUM( lineitem.l_extendedprice *( 1 - lineitem.l_discount )) AS revenue,
    customer.c_acctbal AS c_acctbal,
    nation.n_name AS n_name,
    customer.c_address AS c_address,
    customer.c_phone AS c_phone,
    customer.c_comment AS c_comment
FROM
    customer
JOIN orders ON
    customer.c_custkey = orders.o_custkey
JOIN lineitem ON
    lineitem.l_orderkey = orders.o_orderkey
JOIN nation ON
    customer.c_nationkey = nation.n_nationkey
WHERE
    orders.o_orderdate >= 749404800000
    AND orders.o_orderdate < 757353600000
    AND lineitem.l_returnflag = 'R'
GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
ORDER BY
    revenue DESC LIMIT 20;
