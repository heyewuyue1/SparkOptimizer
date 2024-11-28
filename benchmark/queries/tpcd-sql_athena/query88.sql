select *
from (
    select count(*) filter(where s1) as h8_30_to_9,
        count(*) filter(where s2) as h9_to_9_30,
        count(*) filter(where s3) h9_30_to_10,
        count(*) filter(where s4) h10_to_10_30,
        count(*) filter(where s5) h10_30_to_11,
        count(*) filter(where s6) h11_to_11_30,
        count(*) filter(where s7) h11_30_to_12,
        count(*) filter(where s8) h12_to_12_30
    from(
        select *, (time_dim.t_hour = 8 and time_dim.t_minute >= 30) as s1,
            (time_dim.t_hour = 9 and time_dim.t_minute < 30) as s2,
            (time_dim.t_hour = 9 and time_dim.t_minute >= 30) as s3,
            (time_dim.t_hour = 10 and time_dim.t_minute < 30) as s4,
            (time_dim.t_hour = 10 and time_dim.t_minute >= 30) as s5,
            (time_dim.t_hour = 11 and time_dim.t_minute < 30) as s6,
            (time_dim.t_hour = 11 and time_dim.t_minute >= 30) as s7,
            (time_dim.t_hour = 12 and time_dim.t_minute < 30) as s8
        from store_sales, household_demographics, time_dim, store
        where  ss_sold_time_sk = time_dim.t_time_sk   
            and ss_hdemo_sk = household_demographics.hd_demo_sk 
            and ss_store_sk = s_store_sk 
            and ((household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2) or
          (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
          (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2))
            and store.s_store_name = 'ese'
            AND ((time_dim.t_hour = 8 and time_dim.t_minute >= 30) 
                OR (time_dim.t_hour = 9 and time_dim.t_minute < 30)
                OR (time_dim.t_hour = 9 and time_dim.t_minute >= 30)
                OR (time_dim.t_hour = 10 and time_dim.t_minute < 30)
                OR (time_dim.t_hour = 10 and time_dim.t_minute >= 30)
                OR (time_dim.t_hour = 11 and time_dim.t_minute < 30)
                OR (time_dim.t_hour = 11 and time_dim.t_minute >= 30)
                OR (time_dim.t_hour = 12 and time_dim.t_minute < 30))
    )
)