select  dt.d_year
 	,item.i_brand_id brand_id
 	,item.i_brand brand
 	,sum(ss_ext_sales_price) ext_price
 from date_dim dt
     ,store_sales
     ,item
 where and i_brand_id >= 1001001 and i_brand_id <= 10003004  and  dt.d_date_sk = store_sales.ss_sold_date_sk
    and store_sales.ss_item_sk = item.i_item_sk
    and item.i_manager_id = 1
    and dt.d_moy=12
    and dt.d_year=1998
 group by dt.d_year
 	,item.i_brand
 	,item.i_brand_id
  HAVING ext_price >= 14265205 and ext_price <= 181370904 order by dt.d_year
 	,ext_price desc
 	,brand_id
limit 100 ;