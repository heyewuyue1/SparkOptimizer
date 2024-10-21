select  asceding.rnk, i1.i_product_name best_performing, i2.i_product_name worst_performing
from(select *
     from (select item_sk,rank() over (order by rank_col asc) rnk
           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col
                 from store_sales ss1
                 where ss_store_sk = 236
                 group by ss_item_sk
                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col
                                                  from store_sales
                                                  where ss_store_sk = 236
                                                    and ss_hdemo_sk is null
                                                  group by ss_store_sk))V1)V11
     where rnk  < 11) asceding,
    (select *
     from (select item_sk,rank() over (order by rank_col desc) rnk
           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col
                 from store_sales ss1
                 where ss_store_sk = 236
                 group by ss_item_sk
                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col
                                                  from store_sales
                                                  where ss_store_sk = 236
                                                    and ss_hdemo_sk is null
                                                  group by ss_store_sk))V2)V21
     where rnk  < 11) descending,
item i1,
item i2
where item_sk >= 59694 and item_sk <= 294252 and rnk >= 1 and rnk <= 10 and i_item_sk >= 59694 and i_item_sk <= 294252 and i_current_price >= 0 and i_current_price <= 9 and i_wholesale_cost >= 0 and i_wholesale_cost <= 8 and i_brand_id >= 1004001 and i_brand_id <= 10007014 and i_class_id >= 1 and i_class_id <= 12 and i_category_id >= 1 and i_category_id <= 10 and i_manufact_id >= 208 and i_manufact_id <= 925 and i_manager_id >= 23 and i_manager_id <= 96 and   asceding.rnk = descending.rnk
  and i1.i_item_sk=asceding.item_sk
  and i2.i_item_sk=descending.item_sk
order by asceding.rnk
limit 100;