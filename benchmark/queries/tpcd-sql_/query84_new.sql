select  c_customer_id as customer_id
       , coalesce(c_last_name,'') || ', ' || coalesce(c_first_name,'') as customername
 from customer
     ,customer_address
     ,customer_demographics
     ,household_demographics
     ,income_band
     ,store_returns
 where c_customer_sk >= 9502720 and c_customer_sk <= 9502720 and c_current_cdemo_sk >= 1199668 and c_current_cdemo_sk <= 1199668 and c_current_hdemo_sk >= 5987 and c_current_hdemo_sk <= 5987 and c_current_addr_sk >= 4483506 and c_current_addr_sk <= 4483506 and c_first_shipto_date_sk >= 2451730 and c_first_shipto_date_sk <= 2451730 and c_first_sales_date_sk >= 2451700 and c_first_sales_date_sk <= 2451700 and c_birth_day >= 9 and c_birth_day <= 9 and c_birth_month >= 12 and c_birth_month <= 12 and c_birth_year >= 1925 and c_birth_year <= 1925 and c_last_review_date_sk >= 2452432 and c_last_review_date_sk <= 2452432 and ca_address_sk >= 4483506 and ca_address_sk <= 4483506 and ca_street_number >= 998 and ca_street_number <= 998 and ca_zip >= 60587 and ca_zip <= 60587 and ca_gmt_offset >= -6 and ca_gmt_offset <= -6 and cd_demo_sk >= 1199668 and cd_demo_sk <= 1199668 and cd_purchase_estimate >= 9500 and cd_purchase_estimate <= 9500 and cd_dep_count >= 4 and cd_dep_count <= 4 and cd_dep_employed_count >= 2 and cd_dep_employed_count <= 2 and cd_dep_college_count >= 4 and cd_dep_college_count <= 4 and hd_demo_sk >= 5987 and hd_demo_sk <= 5987 and hd_income_band_sk >= 8 and hd_income_band_sk <= 8 and hd_dep_count >= 9 and hd_dep_count <= 9 and hd_vehicle_count >= 4 and hd_vehicle_count <= 4 and ib_income_band_sk >= 8 and ib_income_band_sk <= 8 and ib_lower_bound >= 70001 and ib_lower_bound <= 70001 and ib_upper_bound >= 80000 and ib_upper_bound <= 80000 and sr_item_sk >= 1195 and sr_item_sk <= 359732 and sr_cdemo_sk >= 1199668 and sr_cdemo_sk <= 1199668 and sr_ticket_number >= 2405722 and sr_ticket_number <= 705549762 and sr_return_tax >= 0 and sr_return_tax <= 356 and sr_net_loss >= 11 and sr_net_loss <= 3804 and sr_returned_date_sk >= 2450867 and sr_returned_date_sk <= 2452750 and   ca_city	        =  'Hopewell'
   and c_current_addr_sk = ca_address_sk
   and ib_lower_bound   >=  32287
   and ib_upper_bound   <=  32287 + 50000
   and ib_income_band_sk = hd_income_band_sk
   and cd_demo_sk = c_current_cdemo_sk
   and hd_demo_sk = c_current_hdemo_sk
   and sr_cdemo_sk = cd_demo_sk
 order by c_customer_id
 limit 100;