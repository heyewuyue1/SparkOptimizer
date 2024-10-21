select
   substr(w_warehouse_name,1,20)
  ,sm_type
  ,web_name
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk <= 30 ) then 1 else 0 end)  as 30days
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 30) and
                 (ws_ship_date_sk - ws_sold_date_sk <= 60) then 1 else 0 end )  as 31_60days
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 60) and
                 (ws_ship_date_sk - ws_sold_date_sk <= 90) then 1 else 0 end)  as 61_90days
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 90) and
                 (ws_ship_date_sk - ws_sold_date_sk <= 120) then 1 else 0 end)  as 91_120days
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk  > 120) then 1 else 0 end)  as gt120days
from
   web_sales
  ,warehouse
  ,ship_mode
  ,web_site
  ,date_dim
where
    d_month_seq between 1212 and 1212 + 11
and ws_ship_date_sk   = d_date_sk
and ws_warehouse_sk   = w_warehouse_sk
and ws_ship_mode_sk   = sm_ship_mode_sk
and ws_web_site_sk    = web_site_sk
group by
   substr(w_warehouse_name,1,20)
  ,sm_type
  ,web_name
 HAVING 30days >= 22084 and 30days <= 89437 and 31_60days >= 21893 and 31_60days <= 89192 and 61_90days >= 21987 and 61_90days <= 89558 and 91_120days >= 21993 and 91_120days <= 89427 and gt120days >= 0 and gt120days <= 0 order by substr(w_warehouse_name,1,20)
        ,sm_type
       ,web_name
limit 100;