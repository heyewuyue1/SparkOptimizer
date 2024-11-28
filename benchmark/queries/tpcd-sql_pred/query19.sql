
select  i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
 	sum(ss_ext_sales_price) ext_price
 from date_dim, store_sales, item,customer,customer_address,store
 where i_brand in ('exportiimporto #1', 'amalgamalg #1', 'amalgscholar #1', 'exportiamalg #1', 'importoexporti #1', 'edu packamalg #1', 'edu packedu pack #2', 'importoexporti #2', 'edu packimporto #1', 'importoscholar #1', 'edu packscholar #1', 'exportischolar #1', 'edu packedu pack #1', 'exportiedu pack #1', 'importoimporto #1', 'edu packimporto #2', 'importoamalg #1', 'maxicorp #7', 'importocorp #1', 'amalgexporti #1', 'exportiimporto #2', 'exportischolar #2', 'exportiexporti #1', 'edu packexporti #2', 'amalgamalg #2', 'importobrand #5', 'edu packunivamalg #16', 'exportiamalg #2', 'edu packbrand #7', 'importoedu pack #2', 'amalgedu pack #2', 'amalgscholar #2', 'amalgcorp #2', 'edu packmaxi #10', 'edu packexporti #1', 'brandunivamalg #4', 'importoamalgamalg #16', 'edu packmaxi #9', 'namelessnameless #9', 'scholarunivamalg #11', 'exportimaxi #2', 'corpcorp #4', 'corpmaxi #5', 'exportiunivamalg #11', 'importoscholar #2', 'amalgamalgamalg #2', 'importoedu pack #1', 'scholaramalgamalg #12', 'corpbrand #6', 'amalgcorp #5') AND  d_date_sk = ss_sold_date_sk
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
 order by ext_price desc
         ,i_brand
         ,i_brand_id
         ,i_manufact_id
         ,i_manufact
limit 100 ;