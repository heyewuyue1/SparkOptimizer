select
         w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
        ,ship_carriers
        ,year
 	,sum(jan_sales) as jan_sales
 	,sum(feb_sales) as feb_sales
 	,sum(mar_sales) as mar_sales
 	,sum(apr_sales) as apr_sales
 	,sum(may_sales) as may_sales
 	,sum(jun_sales) as jun_sales
 	,sum(jul_sales) as jul_sales
 	,sum(aug_sales) as aug_sales
 	,sum(sep_sales) as sep_sales
 	,sum(oct_sales) as oct_sales
 	,sum(nov_sales) as nov_sales
 	,sum(dec_sales) as dec_sales
 	,sum(jan_sales/w_warehouse_sq_ft) as jan_sales_per_sq_foot
 	,sum(feb_sales/w_warehouse_sq_ft) as feb_sales_per_sq_foot
 	,sum(mar_sales/w_warehouse_sq_ft) as mar_sales_per_sq_foot
 	,sum(apr_sales/w_warehouse_sq_ft) as apr_sales_per_sq_foot
 	,sum(may_sales/w_warehouse_sq_ft) as may_sales_per_sq_foot
 	,sum(jun_sales/w_warehouse_sq_ft) as jun_sales_per_sq_foot
 	,sum(jul_sales/w_warehouse_sq_ft) as jul_sales_per_sq_foot
 	,sum(aug_sales/w_warehouse_sq_ft) as aug_sales_per_sq_foot
 	,sum(sep_sales/w_warehouse_sq_ft) as sep_sales_per_sq_foot
 	,sum(oct_sales/w_warehouse_sq_ft) as oct_sales_per_sq_foot
 	,sum(nov_sales/w_warehouse_sq_ft) as nov_sales_per_sq_foot
 	,sum(dec_sales/w_warehouse_sq_ft) as dec_sales_per_sq_foot
 	,sum(jan_net) as jan_net
 	,sum(feb_net) as feb_net
 	,sum(mar_net) as mar_net
 	,sum(apr_net) as apr_net
 	,sum(may_net) as may_net
 	,sum(jun_net) as jun_net
 	,sum(jul_net) as jul_net
 	,sum(aug_net) as aug_net
 	,sum(sep_net) as sep_net
 	,sum(oct_net) as oct_net
 	,sum(nov_net) as nov_net
 	,sum(dec_net) as dec_net
 from (
     select
 	w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
 	,'DIAMOND' || ',' || 'AIRBORNE' as ship_carriers
       ,d_year as year
 	,sum(case when d_moy = 1
 		then ws_sales_price* ws_quantity else 0 end) as jan_sales
 	,sum(case when d_moy = 2
 		then ws_sales_price* ws_quantity else 0 end) as feb_sales
 	,sum(case when d_moy = 3
 		then ws_sales_price* ws_quantity else 0 end) as mar_sales
 	,sum(case when d_moy = 4
 		then ws_sales_price* ws_quantity else 0 end) as apr_sales
 	,sum(case when d_moy = 5
 		then ws_sales_price* ws_quantity else 0 end) as may_sales
 	,sum(case when d_moy = 6
 		then ws_sales_price* ws_quantity else 0 end) as jun_sales
 	,sum(case when d_moy = 7
 		then ws_sales_price* ws_quantity else 0 end) as jul_sales
 	,sum(case when d_moy = 8
 		then ws_sales_price* ws_quantity else 0 end) as aug_sales
 	,sum(case when d_moy = 9
 		then ws_sales_price* ws_quantity else 0 end) as sep_sales
 	,sum(case when d_moy = 10
 		then ws_sales_price* ws_quantity else 0 end) as oct_sales
 	,sum(case when d_moy = 11
 		then ws_sales_price* ws_quantity else 0 end) as nov_sales
 	,sum(case when d_moy = 12
 		then ws_sales_price* ws_quantity else 0 end) as dec_sales
 	,sum(case when d_moy = 1
 		then ws_net_paid_inc_tax * ws_quantity else 0 end) as jan_net
 	,sum(case when d_moy = 2
 		then ws_net_paid_inc_tax * ws_quantity else 0 end) as feb_net
 	,sum(case when d_moy = 3
 		then ws_net_paid_inc_tax * ws_quantity else 0 end) as mar_net
 	,sum(case when d_moy = 4
 		then ws_net_paid_inc_tax * ws_quantity else 0 end) as apr_net
 	,sum(case when d_moy = 5
 		then ws_net_paid_inc_tax * ws_quantity else 0 end) as may_net
 	,sum(case when d_moy = 6
 		then ws_net_paid_inc_tax * ws_quantity else 0 end) as jun_net
 	,sum(case when d_moy = 7
 		then ws_net_paid_inc_tax * ws_quantity else 0 end) as jul_net
 	,sum(case when d_moy = 8
 		then ws_net_paid_inc_tax * ws_quantity else 0 end) as aug_net
 	,sum(case when d_moy = 9
 		then ws_net_paid_inc_tax * ws_quantity else 0 end) as sep_net
 	,sum(case when d_moy = 10
 		then ws_net_paid_inc_tax * ws_quantity else 0 end) as oct_net
 	,sum(case when d_moy = 11
 		then ws_net_paid_inc_tax * ws_quantity else 0 end) as nov_net
 	,sum(case when d_moy = 12
 		then ws_net_paid_inc_tax * ws_quantity else 0 end) as dec_net
     from
          web_sales
         ,warehouse
         ,date_dim
         ,time_dim
 	  ,ship_mode
     where
            ws_warehouse_sk =  w_warehouse_sk
        and ws_sold_date_sk = d_date_sk
        and ws_sold_time_sk = t_time_sk
 	and ws_ship_mode_sk = sm_ship_mode_sk
        and d_year = 2002
 	and t_time between 49530 and 49530+28800
 	and sm_carrier in ('DIAMOND','AIRBORNE')
     group by
        w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
       ,d_year
 union all
     select
 	w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
 	,'DIAMOND' || ',' || 'AIRBORNE' as ship_carriers
       ,d_year as year
 	,sum(case when d_moy = 1
 		then cs_ext_sales_price* cs_quantity else 0 end) as jan_sales
 	,sum(case when d_moy = 2
 		then cs_ext_sales_price* cs_quantity else 0 end) as feb_sales
 	,sum(case when d_moy = 3
 		then cs_ext_sales_price* cs_quantity else 0 end) as mar_sales
 	,sum(case when d_moy = 4
 		then cs_ext_sales_price* cs_quantity else 0 end) as apr_sales
 	,sum(case when d_moy = 5
 		then cs_ext_sales_price* cs_quantity else 0 end) as may_sales
 	,sum(case when d_moy = 6
 		then cs_ext_sales_price* cs_quantity else 0 end) as jun_sales
 	,sum(case when d_moy = 7
 		then cs_ext_sales_price* cs_quantity else 0 end) as jul_sales
 	,sum(case when d_moy = 8
 		then cs_ext_sales_price* cs_quantity else 0 end) as aug_sales
 	,sum(case when d_moy = 9
 		then cs_ext_sales_price* cs_quantity else 0 end) as sep_sales
 	,sum(case when d_moy = 10
 		then cs_ext_sales_price* cs_quantity else 0 end) as oct_sales
 	,sum(case when d_moy = 11
 		then cs_ext_sales_price* cs_quantity else 0 end) as nov_sales
 	,sum(case when d_moy = 12
 		then cs_ext_sales_price* cs_quantity else 0 end) as dec_sales
 	,sum(case when d_moy = 1
 		then cs_net_paid_inc_ship_tax * cs_quantity else 0 end) as jan_net
 	,sum(case when d_moy = 2
 		then cs_net_paid_inc_ship_tax * cs_quantity else 0 end) as feb_net
 	,sum(case when d_moy = 3
 		then cs_net_paid_inc_ship_tax * cs_quantity else 0 end) as mar_net
 	,sum(case when d_moy = 4
 		then cs_net_paid_inc_ship_tax * cs_quantity else 0 end) as apr_net
 	,sum(case when d_moy = 5
 		then cs_net_paid_inc_ship_tax * cs_quantity else 0 end) as may_net
 	,sum(case when d_moy = 6
 		then cs_net_paid_inc_ship_tax * cs_quantity else 0 end) as jun_net
 	,sum(case when d_moy = 7
 		then cs_net_paid_inc_ship_tax * cs_quantity else 0 end) as jul_net
 	,sum(case when d_moy = 8
 		then cs_net_paid_inc_ship_tax * cs_quantity else 0 end) as aug_net
 	,sum(case when d_moy = 9
 		then cs_net_paid_inc_ship_tax * cs_quantity else 0 end) as sep_net
 	,sum(case when d_moy = 10
 		then cs_net_paid_inc_ship_tax * cs_quantity else 0 end) as oct_net
 	,sum(case when d_moy = 11
 		then cs_net_paid_inc_ship_tax * cs_quantity else 0 end) as nov_net
 	,sum(case when d_moy = 12
 		then cs_net_paid_inc_ship_tax * cs_quantity else 0 end) as dec_net
     from
          catalog_sales
         ,warehouse
         ,date_dim
         ,time_dim
 	 ,ship_mode
     where
            cs_warehouse_sk =  w_warehouse_sk
        and cs_sold_date_sk = d_date_sk
        and cs_sold_time_sk = t_time_sk
 	and cs_ship_mode_sk = sm_ship_mode_sk
        and d_year = 2002
 	and t_time between 49530 AND 49530+28800
 	and sm_carrier in ('DIAMOND','AIRBORNE')
     group by
        w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
       ,d_year
 ) x
 group by
        w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
 	,ship_carriers
       ,year
  HAVING jan_sales >= 14562034282 and jan_sales <= 14927705399 and feb_sales >= 13256439444 and feb_sales <= 13506542733 and mar_sales >= 14162547901 and mar_sales <= 14514425692 and apr_sales >= 14154300402 and apr_sales <= 14512601328 and may_sales >= 14486937775 and may_sales <= 14927228163 and jun_sales >= 14150466934 and jun_sales <= 14383100011 and jul_sales >= 14622295387 and jul_sales <= 14965039543 and aug_sales >= 32774194722 and aug_sales <= 33242700376 and sep_sales >= 32310973648 and sep_sales <= 32813617329 and oct_sales >= 33463527873 and oct_sales <= 33966057408 and nov_sales >= 48222552596 and nov_sales <= 49121120157 and dec_sales >= 50706766298 and dec_sales <= 51137967972 and jan_net >= 29172074406 and jan_net <= 29646050292 and feb_net >= 25161735480 and feb_net <= 25577282546 and mar_net >= 26950374164 and mar_net <= 27554557232 and apr_net >= 26962631449 and apr_net <= 27357296858 and may_net >= 27658553976 and may_net <= 28334659192 and jun_net >= 26846661763 and jun_net <= 27496455933 and jul_net >= 27802789603 and jul_net <= 28506181410 and aug_net >= 62137672849 and aug_net <= 62808779670 and sep_net >= 61285869344 and sep_net <= 62492687307 and oct_net >= 63681134049 and oct_net <= 64460625788 and nov_net >= 91690277804 and nov_net <= 92914450551 and dec_net >= 96524730011 and dec_net <= 97132526855 order by w_warehouse_name
 limit 100;