select
  c_last_name,c_first_name,substr(s_city,1,30),ss_ticket_number,amt,profit
  from
   (select ss_ticket_number
          ,ss_customer_sk
          ,store.s_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
    from store_sales,date_dim,store,household_demographics
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and (household_demographics.hd_dep_count = 8 or household_demographics.hd_vehicle_count > 0)
    and date_dim.d_dow = 1
    and date_dim.d_year in (1998,1998+1,1998+2)
    and store.s_number_employees between 200 and 295
    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,store.s_city) ms,customer
    where ss_ticket_number >= 14987481 and ss_ticket_number <= 713318151 and ss_customer_sk >= 430899 and ss_customer_sk <= 29590755 and c_customer_sk >= 430899 and c_customer_sk <= 29590755 and c_current_addr_sk >= 283889 and c_current_addr_sk <= 14795124 and   ss_customer_sk = c_customer_sk
 order by c_last_name,c_first_name,substr(s_city,1,30), profit
limit 100;