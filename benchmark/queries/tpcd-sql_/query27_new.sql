select  i_item_id,
        s_state, grouping(s_state) g_state,
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4
 from store_sales, customer_demographics, date_dim, store, item
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_store_sk = s_store_sk and
       ss_cdemo_sk = cd_demo_sk and
       cd_gender = 'F' and
       cd_marital_status = 'M' and
       cd_education_status = '4 yr Degree' and
       d_year = 2002 and
       s_state in ('NE','IN', 'SD', 'MN', 'TX', 'MN')
 group by rollup (i_item_id, s_state)
  HAVING g_state >= 0 and g_state <= 1 and agg1 >= 17 and agg1 <= 94 and agg2 >= 7 and agg2 <= 145 and agg3 >= 0 and agg3 <= 5211 and agg4 >= 4 and agg4 <= 101 order by i_item_id
         ,s_state
 limit 100;