select  s_store_name, s_store_id,
        sum(case when (d_day_name='Sunday') then ss_sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then ss_sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then ss_sales_price else  null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then ss_sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then ss_sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then ss_sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then ss_sales_price else null end) sat_sales
 from date_dim, store_sales, store
 where d_date_sk = ss_sold_date_sk and
       s_store_sk = ss_store_sk and
       s_gmt_offset = -6 and
       d_year = 1998
 group by s_store_name, s_store_id
  HAVING sun_sales >= 12237632 and sun_sales <= 12643987 and mon_sales >= 12367123 and mon_sales <= 12662700 and tue_sales >= 12620839 and tue_sales <= 13031240 and wed_sales >= 12655560 and wed_sales <= 13047712 and thu_sales >= 12634569 and thu_sales <= 12992267 and fri_sales >= 12300046 and fri_sales <= 12647350 and sat_sales >= 12265362 and sat_sales <= 12649804 order by s_store_name, s_store_id,sun_sales,mon_sales,tue_sales,wed_sales,thu_sales,fri_sales,sat_sales
 limit 100;