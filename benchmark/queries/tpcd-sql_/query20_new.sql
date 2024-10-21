select  i_item_id
       ,i_item_desc
       ,i_category
       ,i_class
       ,i_current_price
       ,sum(cs_ext_sales_price) as itemrevenue
       ,sum(cs_ext_sales_price)*100/sum(sum(cs_ext_sales_price)) over
           (partition by i_class) as revenueratio
 from	catalog_sales
     ,item
     ,date_dim
 where cs_item_sk = i_item_sk
   and i_category in ('Jewelry', 'Sports', 'Books')
   and cs_sold_date_sk = d_date_sk
 and d_date between cast('2001-01-12' as date)
 				and (cast('2001-01-12' as date) + interval 30 days)
 group by i_item_id
         ,i_item_desc
         ,i_category
         ,i_class
         ,i_current_price
  HAVING itemrevenue >= 470566 and itemrevenue <= 709024 and revenueratio >= 0 and revenueratio <= 2 order by i_category
         ,i_class
         ,i_item_id
         ,i_item_desc
         ,revenueratio
limit 100;