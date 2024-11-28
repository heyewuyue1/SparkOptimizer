select case when v1 > 48409437 then t1 else e1 end,
    case when v2 > 24804257 then t2 else e2 end,
    case when v3 > 128048939 then t3 else e3 end,
    case when v4 > 56503968 then t4 else e4 end,
    case when v5 > 43571537 then t5 else e5 end

from(
    select count(*) filter(where b1) as v1,
            avg(ss_ext_discount_amt) filter(where b1) as t1,
            avg(ss_net_profit) filter(where b1) as e1,
            count(*) filter(where b2) as v2,
            avg(ss_ext_discount_amt) filter(where b2) as t2,
            avg(ss_net_profit) filter(where b2) as e2,
            count(*) filter(where b3) as v3,
            avg(ss_ext_discount_amt) filter(where b3) as t3,
            avg(ss_net_profit) filter(where b3) as e3,
            count(*) filter(where b4) as v4,
            avg(ss_ext_discount_amt) filter(where b4) as t4,
            avg(ss_net_profit) filter(where b4) as e4,
            count(*) filter(where b5) as v5,
            avg(ss_ext_discount_amt) filter(where b5) as t5,
            avg(ss_net_profit) filter(where b5) as e5
    from(
        select *,ss_quantity between 1 and 20 as b1,
            ss_quantity between 21 and 40 as b2,
            ss_quantity between 41 and 60 as b3,
            ss_quantity between 61 and 80 as b4,
            ss_quantity between 81 and 100 as b5
        from store_sales
        where ss_quantity between 1 and 20
            or ss_quantity between 21 and 40
            or ss_quantity between 41 and 60
            or ss_quantity between 61 and 80
            or ss_quantity between 81 and 100)), reason

where r_reason_sk = 1;