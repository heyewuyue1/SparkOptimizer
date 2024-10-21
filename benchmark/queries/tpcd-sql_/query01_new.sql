with customer_total_return as
(select sr_customer_sk as ctr_customer_sk
,sr_store_sk as ctr_store_sk
,sum(SR_FEE) as ctr_total_return
from store_returns
,date_dim
where sr_returned_date_sk = d_date_sk
and d_year =2000
group by sr_customer_sk
,sr_store_sk)
 select  c_customer_id
from customer_total_return ctr1
,store
,customer
where ctr_customer_sk >= 4096 and ctr_customer_sk <= 29753344 and ctr_store_sk >= 2 and ctr_store_sk <= 1345 and ctr_total_return >= 61 and ctr_total_return <= 139 and s_store_sk >= 2 and s_store_sk <= 1345 and s_number_employees >= 202 and s_number_employees <= 300 and s_floor_space >= 5018838 and s_floor_space <= 9963406 and s_market_id >= 1 and s_market_id <= 10 and s_division_id >= 1 and s_division_id <= 1 and s_company_id >= 1 and s_company_id <= 1 and s_street_number >= 7 and s_street_number <= 984 and s_zip >= 30162 and s_zip <= 39532 and s_gmt_offset >= -6 and s_gmt_offset <= -5 and s_tax_precentage >= 0 and s_tax_precentage <= 1 and c_customer_sk >= 4096 and c_customer_sk <= 29753344 and c_current_addr_sk >= 71894 and c_current_addr_sk <= 14976523 and   ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
from customer_total_return ctr2
where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
and s_store_sk = ctr1.ctr_store_sk
and s_state = 'TN'
and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
limit 100;