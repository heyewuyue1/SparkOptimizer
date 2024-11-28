with customer_total_return AS (
  SELECT 
    wr_returning_customer_sk AS ctr_customer_sk,
    ca_state AS ctr_state,
    SUM(wr_return_amt) AS ctr_total_return
  FROM 
    web_returns
    JOIN date_dim ON wr_returned_date_sk = d_date_sk
    JOIN customer_address ON wr_returning_addr_sk = ca_address_sk
  WHERE 
    d_year = 2002
  GROUP BY 
    wr_returning_customer_sk, ca_state
  )
,
ctr1 AS(
  SELECT * FROM (
    SELECT ctr_total_return,ctr_customer_sk,AVG(ctr_total_return) OVER (PARTITION BY ctr_state) AS avg_return_threshold
    FROM customer_total_return
  )
  WHERE ctr_total_return > avg_return_threshold * 1.2
  )

SELECT 
  c_customer_id, 
  c_salutation, 
  c_first_name, 
  c_last_name, 
  c_preferred_cust_flag, 
  c_birth_day, 
  c_birth_month, 
  c_birth_year, 
  c_birth_country, 
  c_login, 
  c_email_address, 
  c_last_review_date_sk,
  ctr1.ctr_total_return
FROM 
  ctr1,
  customer_address ca,
  customer c
WHERE 
  ctr1.ctr_customer_sk = c_customer_sk
  AND ca_state = 'IL'
  AND ca_address_sk = c_current_addr_sk
ORDER BY 
  c_customer_id, 
  c_salutation, 
  c_first_name, 
  c_last_name, 
  c_preferred_cust_flag, 
  c_birth_day, 
  c_birth_month, 
  c_birth_year, 
  c_birth_country, 
  c_login, 
  c_email_address, 
  c_last_review_date_sk, 
  ctr1.ctr_total_return
LIMIT 100;