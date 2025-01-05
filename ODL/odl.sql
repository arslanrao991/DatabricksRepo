-- Databricks notebook source
SELECT count(1)
  FROM databricks_practice.rdl.actions


-- COMMAND ----------

-------- E2
WITH segmentation AS (
SELECT  user_id,
        CASE 
             WHEN MIN(action_ts) = MAX(action_ts) 
             OR DATEDIFF(day, MIN(action_ts),'2018-06-06') <= 7 THEN 'trial' 

             WHEN DATEDIFF(day, MIN(action_ts),'2018-06-06') > 7 
             AND DATEDIFF(day, MIN(action_ts),'2018-06-06') <= 56 THEN 'novice'

             WHEN DATEDIFF(day, MIN(action_ts), '2018-06-06') > 56 
             AND COUNT(CASE WHEN action_ts > DATEADD(day, -28, '2018-06-06') THEN 1 END)>0 THEN 'repeat'

             WHEN DATEDIFF(day, MIN(action_ts), '2018-06-06') > 56
             AND COUNT(CASE WHEN action_ts > DATEADD(day, -84, '2018-06-06') THEN 1 END)=0 THEN 'lost' 

             WHEN DATEDIFF(day, MIN(action_ts), '2018-06-06') > 56 
             AND COUNT(CASE WHEN action_ts > DATEADD(day, -28, '2018-06-06') THEN 1 END)=0 THEN 'dormant'

        END AS segment,
        DATEDIFF(day, MIN(action_ts),'2018-06-06') AS DD

  FROM  databricks_practice.rdl.actions
--  WHERE  action_ts > '2018-06-30'
 GROUP  BY user_id
)
SELECT distinct segment
FROM segmentation
join databricks_practice.rdl.actions using (user_id);
-- where user_id= '0b5b5c6fe5322d163da4aceabda30d22'

-- COMMAND ----------

-------- E3
WITH dim_dates AS (
	SELECT distinct action_ts ::DATE as date_nk, date_nk ||'|'|| 'id' as date_sk
        FROM databricks_practice.rdl.actions
),
dim_items AS (
      SELECT distinct item_id as item_nk, item_nk ||'|'|| 'id' as item_sk
        FROM databricks_practice.rdl.actions
),
items_posted AS (
      SELECT item_id, action_ts :: DATE as posted_date, device, b2c
        FROM databricks_practice.rdl.actions a 
       WHERE action_type = 'P' --and item_id = 'f1f31ba58290fb445473cac13cd7b863'
),
fact_item_liquidity AS (
      SELECT 
             a.item_id,
             SUM(CASE WHEN DATEDIFF(day, ip.posted_date :: DATE, a.action_ts :: DATE) = 1 THEN 1 ELSE 0 END) AS replies_1day,

             SUM(CASE WHEN DATEDIFF(day, ip.posted_date :: DATE, a.action_ts :: DATE) < 8 THEN 1 ELSE 0 END) AS replies_7day

        FROM databricks_practice.rdl.actions a 
       INNER JOIN items_posted ip 
             ON a.item_id = ip.item_id
       WHERE a.action_type = 'R'
       GROUP BY a.item_id
)

SELECT ip.posted_date,
       COUNT(*) total_items_posted,
       COUNT(CASE WHEN replies_1day>=1 THEN 1 END) as liquid_items_1r_1d,
       COUNT(CASE WHEN replies_7day>=3 THEN 1 END) as liquid_items_3r_7d,
       COUNT(CASE WHEN replies_7day>=5 THEN 1 END) as liquid_items_5r_7d
       
  FROM items_posted ip 
  LEFT JOIN fact_item_liquidity il on ip.item_id=il.item_id
 GROUP BY ip.posted_date
 ORDER BY ip.posted_date


      --  select *
      --  from actions
      --  where item_id = 'f1f31ba58290fb445473cac13cd7b863'
      --  group by 1
      --  having count(1)>2
