Q1:--Kết quả test Postgresql:
	order_id	pickup_attempt_timestamp	pickup_attempt_description
	123456	               	                                    x
        246357	                x	                            x
       2323323	               	                                    x
--Orders có mô tả successful pickup thì vẫn có khả năng là abnormal case đúng không anh?
SELECT
order_id,
CASE WHEN pickup_attempt_description = 'Successful Pickup' THEN 'x' ELSE ' ' END AS is_succesful_pick_up,
CASE WHEN 
     EXTRACT(minute FROM MAX(pickup_attempt_timestamp) - MIN(pickup_attempt_timestamp)) < 2
     OR COUNT(DISTINCT pickup_attempt_timestamp) < 6
     OR COUNT(DISTINCT EXTRACT(DAY FROM pickup_attempt_timestamp)) < 2 
     THEN 'x' ELSE ' ' END AS is_abnormal
FROM pickup_attempts
GROUP BY order_id, is_succesful_pick_up
  

Q2: --Kết quả test Postgresql:
	agent_name      ontime_reply_percentage
	Nguyen Van A	  0.00%
        Nguyen Van B	  100.00%
WITH tab1 AS
(
SELECT
  *, 
  ROW_NUMBER () OVER(PARTITION BY email_group_id ORDER BY email_send_timestamp) AS rank
FROM email_contacts
)
, tab2 AS
(
SELECT *,
CASE WHEN
        EXTRACT(hour FROM (agent_reply_timestamp - email_send_timestamp)) <= 6
        AND EXTRACT (day FROM (agent_reply_timestamp - email_send_timestamp)) < 1
        AND rank = 1 
	 THEN 'x' ELSE NULL END AS ontime_reply
FROM tab1 
WHERE rank = 1
)
SELECT 
  tab2.agent_name,
  ROUND(COUNT(tab2.ontime_reply)*100.00/COUNT(DISTINCT tab1.email_group_id),2) || '%' AS ontime_reply_percentage
FROM tab1 RIGHT JOIN tab2 ON tab1.email_group_id = tab2.email_group_id AND tab1.rank = tab2.rank
GROUP BY tab2.agent_name

	
Q3: --Kết quả test Postgresql: 0 rows, hoan hỉ hoan hỉ :)))
--CTE of weekly FD rate
WITH tab1 AS
(
SELECT
  grass_date,
  'week' AS key_type,
  shipping_carrier,
  SUM(nb_failed_deli)*100.00 / SUM(nb_create_order) AS failed_deli_rate
FROM failed_delivery
WHERE grass_date >= CURRENT_DATE - INTERVAL '3 months'
GROUP BY grass_date, shipping_carrier   --em thấy đoạn này chắc sai sai :)
ORDER BY grass_date, shipping_carrier
)
--CTE of monthly FD rate
, tab2 AS 
(
SELECT
  grass_date,
  'month' AS key_type,
  shipping_carrier,
  SUM(nb_failed_deli)*100.00 / SUM(nb_create_order) AS failed_deli_rate
FROM failed_delivery
WHERE grass_date >= CURRENT_DATE - INTERVAL '3 months'
GROUP BY grass_date, shipping_carrier     --em thấy đoạn này chắc sai sai :)
ORDER BY grass_date, shipping_carrier
)
SELECT * FROM tab1
UNION ALL 
SELECT * FROM tab2
ORDER BY  grass_date, shipping_carrier


Q4:--Kết quả test Postgresql:
	buyer_id    avg_between_purchases
	a	    19 days 15:35:21.5
        b	    1 day 10:42:34
	
WITH tab AS
(SELECT
  buyer_id, 
  purchase_timestamp - LAG(purchase_timestamp) OVER(PARTITION BY buyer_id ORDER BY purchase_timestamp) AS timediff
  FROM buyer_purchase
 )
SELECT 
  buyer_id,
  AVG(timediff) AS avg_between_purchases
FROM tab
GROUP BY buyer_id


Q5:--kết quả test Postgresql ERROR:  division by zero
WITH nps AS 
(
SELECT 
    EXTRACT(month FROM 	submit_timestamp) AS month,
	survey_type,
	COUNT(DISTINCT user_id) AS total_buyers,
    (COUNT(CASE WHEN score IN (9, 10) THEN 1 END) - 
	 COUNT(CASE WHEN score >= 0 AND score <= 6 THEN 1 END))*100.00/COUNT(*) AS nps_score
FROM nps_raw
WHERE survey_type IN ('A','B','C')
  AND submit_timestamp >= '2023-01-01'
  AND submit_timestamp <= '2023-06-30'
  AND is_seller = 0
GROUP BY EXTRACT(month FROM submit_timestamp), survey_type
)
, retention AS
(
SELECT
	EXTRACT(month FROM n.submit_timestamp) AS month,
	n.survey_type,
	COUNT(DISTINCT buyer_id) AS return_buyers --returning buyers
FROM order_mart AS o JOIN nps_raw AS n ON o.order_id = n.order_id
WHERE n.survey_type IN ('A','B','C')
  AND o.create_timestamp <= n.submit_timestamp + INTERVAL '30 days'
  AND n.submit_timestamp >= '2023-01-01'
  AND n.submit_timestamp <= '2023-06-30'
  AND n.is_seller = 0
GROUP BY EXTRACT(month FROM n.submit_timestamp), n.survey_type
)
SELECT
    n.month,
	n.survey_type,
	n.nps_score,
	r.return_buyers*100.00/n.total_buyers AS retention_rate
FROM nps n LEFT JOIN retention r ON n.month = r.month AND n.survey_type = r.survey_type
ORDER BY n.survey_type, n.month






















