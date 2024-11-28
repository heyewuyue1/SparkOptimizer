SELECT s_store_name,
       i_item_desc,
       sc.revenue,
       i_current_price,
       i_wholesale_cost,
       i_brand
FROM store, item,
     (SELECT ss_store_sk,
             ss_item_sk,
             revenue,
             AVG(revenue) OVER (PARTITION BY ss_store_sk) avgR
      FROM (SELECT ss_store_sk,
                   ss_item_sk,
                   SUM(ss_sales_price) AS revenue
            FROM store_sales, date_dim
            WHERE ss_sold_date_sk = d_date_sk
              AND d_month_seq BETWEEN 1212 AND 1212 + 11
            GROUP BY ss_store_sk, ss_item_sk) sb) sc
WHERE sc.revenue <= 0.1 * avgR
  AND s_store_sk = sc.ss_store_sk
  AND i_item_sk = sc.ss_item_sk
ORDER BY s_store_name, i_item_desc
LIMIT 100;
