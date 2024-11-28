WITH customer_total_return AS (
 SELECT sr_customer_sk AS ctr_customer_sk,
 sr_store_sk AS ctr_store_sk,
 sum(sr_return_amt) AS ctr_total_return
 FROM store_returns, date_dim
 WHERE sr_returned_date_sk = d_date_sk 
 AND d_year = 2000
 GROUP BY sr_customer_sk, sr_store_sk)
SELECT c_customer_id
FROM store, 
 customer, 
 (SELECT *, 
 1.2 * AVG(ctr_total_Return) OVER
 (PARTITION BY ctr_store_sk) AS aCtr 
 FROM customer_total_return) ctr
WHERE ctr.ctr_total_return > ctr.aCtr
 AND s_store_sk = ctr.ctr_store_sk
AND s_state = 'TN'
AND ctr.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id LIMIT 100; 