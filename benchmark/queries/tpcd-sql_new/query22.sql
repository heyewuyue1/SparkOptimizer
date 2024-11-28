select  i_product_name
             ,i_brand
             ,i_class
             ,i_category
             ,avg(inv_quantity_on_hand) qoh
       from inventory
           ,date_dim
           ,item
       where i_product_name in ('ationeseoughtcallyantiought', 'ationcallyoughtpribarought', 'priationbareingation', 'ationationeingation', 'oughteingpriprin stable', 'priableprioughtesepri', 'antibarpriantiableought', 'ationbarcallyprin stable', 'oughteingpripriableought', 'n stcallyoughtbarpripri', 'oughtantin stn stoughtable', 'antiablebarationpriought', 'antiantin stoughtable', 'oughtesecallyationese', 'n stn stantiantiantiought', 'oughtationprieingbarable', 'n stationationationation', 'ationesecallycallyableought', 'ationationeingeseationought', 'antibarbarcallyeseable', 'antiprieingantiation', 'ationationoughtantiese', 'oughtationanticallybarable', 'antieseoughtcallyeing', 'prieingpribareseought') and   inv_date_sk=d_date_sk
              and inv_item_sk=i_item_sk
              and d_month_seq between 1212 and 1212 + 11
       group by rollup(i_product_name
                       ,i_brand
                       ,i_class
                       ,i_category)
 HAVING qoh >= 463 and qoh <= 469 order by qoh, i_product_name, i_brand, i_class, i_category
limit 100;