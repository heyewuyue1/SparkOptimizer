select  c_last_name
       ,c_first_name
       ,ca_city
       ,bought_city
       ,ss_ticket_number
       ,amt,profit
 from
   (select ss_ticket_number
          ,ss_customer_sk
          ,ca_city bought_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
    from store_sales,date_dim,store,household_demographics,customer_address
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and store_sales.ss_addr_sk = customer_address.ca_address_sk
    and (household_demographics.hd_dep_count = 3 or
         household_demographics.hd_vehicle_count= -1)
    and date_dim.d_dow in (6,0)
    and date_dim.d_year in (2000,2000+1,2000+2)
    and store.s_city in ('Pine Grove','Oak Ridge','Hamilton','Salem','Stringtown')
    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,ca_city) dn,customer,customer_address current_addr
    where ss_ticket_number >= 2360254 and ss_ticket_number <= 709782005 and ss_customer_sk >= 218787 and ss_customer_sk <= 28228115 and amt >= 0 and amt <= 16286 and profit >= -35392 and profit <= 7595 and c_customer_sk >= 218787 and c_customer_sk <= 28228115 and c_current_addr_sk >= 251320 and c_current_addr_sk <= 14963710 and ca_address_sk >= 251320 and ca_address_sk <= 14963710 and   ss_customer_sk = c_customer_sk
      and customer.c_current_addr_sk = current_addr.ca_address_sk
      and current_addr.ca_city <> bought_city
  order by c_last_name
          ,c_first_name
          ,ca_city
          ,bought_city
          ,ss_ticket_number
  limit 100;