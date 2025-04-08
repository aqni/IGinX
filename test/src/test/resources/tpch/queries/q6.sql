SELECT
    SUM( l_extendedprice * l_discount ) AS revenue
FROM
    lineitem
WHERE
    l_shipdate >= 757353600000
    AND l_shipdate < 788889600000
    AND l_discount >= 0.05
    AND l_discount <= 0.07
    AND l_quantity < 24;
