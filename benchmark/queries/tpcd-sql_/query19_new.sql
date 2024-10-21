select  i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
 	sum(ss_ext_sales_price) ext_price
 from date_dim, store_sales, item,customer,customer_address,store
 where i_brand_id >= 1001001 and i_brand_id <= 10015012 and i_manufact_id >= 9 and i_manufact_id <= 859 and d_date_sk = ss_sold_date_sk
   and ss_item_sk = i_item_sk
   and i_manager_id=7
   and d_moy=11
   and d_year=1999
   and ss_customer_sk = c_customer_sk
   and c_current_addr_sk = ca_address_sk
   and substr(ca_zip,1,5) <> substr(s_zip,1,5)
   and ss_store_sk = s_store_sk
 group by i_brand
      ,i_brand_id
      ,i_manufact_id
      ,i_manufact
  HAVING ext_price >= 2851376 and ext_price <= 7726578 order by ext_price desc
         ,i_brand
         ,i_brand_id
         ,i_manufact_id
         ,i_manufact
limit 100 ;