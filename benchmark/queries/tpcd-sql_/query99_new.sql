select
   substr(w_warehouse_name,1,20)
  ,sm_type
  ,cc_name
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk <= 30 ) then 1 else 0 end)  as 30days
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 30) and
                 (cs_ship_date_sk - cs_sold_date_sk <= 60) then 1 else 0 end )  as 31_60days
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 60) and
                 (cs_ship_date_sk - cs_sold_date_sk <= 90) then 1 else 0 end)  as 61_90days
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 90) and
                 (cs_ship_date_sk - cs_sold_date_sk <= 120) then 1 else 0 end)  as 91_120days
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk  > 120) then 1 else 0 end)  as gt120days
from
   catalog_sales
  ,warehouse
  ,ship_mode
  ,call_center
  ,date_dim
where
    d_month_seq between 1212 and 1212 + 11
and cs_ship_date_sk   = d_date_sk
and cs_warehouse_sk   = w_warehouse_sk
and cs_ship_mode_sk   = sm_ship_mode_sk
and cs_call_center_sk = cc_call_center_sk
group by
   substr(w_warehouse_name,1,20)
  ,sm_type
  ,cc_name
 HAVING 30days >= 78179 and 30days <= 105596 and 31_60days >= 80643 and 31_60days <= 109782 and 61_90days >= 81041 and 61_90days <= 109700 and 91_120days >= 0 and 91_120days <= 0 and gt120days >= 0 and gt120days <= 0 order by substr(w_warehouse_name,1,20)
        ,sm_type
        ,cc_name
limit 100;