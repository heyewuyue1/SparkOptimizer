with wscs as
 (select sold_date_sk
        ,sales_price
  from (select ws_sold_date_sk sold_date_sk
              ,ws_ext_sales_price sales_price
        from web_sales
        union all
        select cs_sold_date_sk sold_date_sk
              ,cs_ext_sales_price sales_price
        from catalog_sales)),
 wswscs as
 (select d_week_seq,
        sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
 from wscs
     ,date_dim
 where d_date_sk = sold_date_sk
 group by d_week_seq)
 select d_week_seq1
       ,round(sun_sales1/sun_sales2,2)
       ,round(mon_sales1/mon_sales2,2)
       ,round(tue_sales1/tue_sales2,2)
       ,round(wed_sales1/wed_sales2,2)
       ,round(thu_sales1/thu_sales2,2)
       ,round(fri_sales1/fri_sales2,2)
       ,round(sat_sales1/sat_sales2,2)
 from
 (select wswscs.d_week_seq d_week_seq1
        ,sun_sales sun_sales1
        ,mon_sales mon_sales1
        ,tue_sales tue_sales1
        ,wed_sales wed_sales1
        ,thu_sales thu_sales1
        ,fri_sales fri_sales1
        ,sat_sales sat_sales1
  from wswscs,date_dim
  where date_dim.d_week_seq = wswscs.d_week_seq and
        d_year = 2001) y,
 (select wswscs.d_week_seq d_week_seq2
        ,sun_sales sun_sales2
        ,mon_sales mon_sales2
        ,tue_sales tue_sales2
        ,wed_sales wed_sales2
        ,thu_sales thu_sales2
        ,fri_sales fri_sales2
        ,sat_sales sat_sales2
  from wswscs
      ,date_dim
  where date_dim.d_week_seq = wswscs.d_week_seq and
        d_year = 2001+1) z
 where d_week_seq1 >= 5270 and d_week_seq1 <= 5322 and sun_sales1 >= 5194596652 and sun_sales1 <= 17959072460 and mon_sales1 >= 5188056894 and mon_sales1 <= 17968373012 and tue_sales1 >= 5190681334 and tue_sales1 <= 17970427597 and wed_sales1 >= 5178470500 and wed_sales1 <= 17972632135 and thu_sales1 >= 1741634776 and thu_sales1 <= 17991651892 and fri_sales1 >= 3458433775 and fri_sales1 <= 17971222572 and sat_sales1 >= 5185057181 and sat_sales1 <= 17972232949 and d_week_seq2 >= 5323 and d_week_seq2 <= 5375 and sun_sales2 >= 3467058635 and sun_sales2 <= 17973990837 and mon_sales2 >= 3466871578 and mon_sales2 <= 17956056297 and tue_sales2 >= 5190775629 and tue_sales2 <= 17979585747 and wed_sales2 >= 5179825171 and wed_sales2 <= 17967083561 and thu_sales2 >= 5190385985 and thu_sales2 <= 17976277329 and fri_sales2 >= 1753626904 and fri_sales2 <= 17970577498 and sat_sales2 >= 3452986026 and sat_sales2 <= 17964861562 and   d_week_seq1=d_week_seq2-53
 order by d_week_seq1;