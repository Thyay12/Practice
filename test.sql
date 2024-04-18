Q1:
SELECT
order_id,
CASE WHEN pickup_attempt_description = 'Successful Pickup' THEN 'x' ELSE ' ' AS is_succesful_pick_up
CASE WHEN 
         EXTRACT(minute FROM TIMEDIFF(MAX(pickup_attempt_timestamp) - MIN(pickup_attempt_timestamp))) < 2
     OR COUNT(DISTINCT pickup_attempt_timestamp) < 6
     OR COUNT(DISTINCT EXTRACT(date FROM pickup_attempt_timestamp)) < 2 
     THEN 'x' ELSE ' ' AS is_abnormal
FROM pickup_attempts
GROUP BY order_id
  

Q2:
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

Q3:

