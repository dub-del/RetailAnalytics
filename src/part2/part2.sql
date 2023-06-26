-- 2.1. Customers
-- DROP TABLE IF EXISTS segments CASCADE;
CREATE TABLE IF NOT EXISTS segments
(
    Segment                SERIAL PRIMARY KEY,
    Average_check          varchar NOT NULL,
    Frequency_of_purchases varchar NOT NULL,
    Churn_probability      varchar NOT NULL
);
CALL import('segments', 'Segments.tsv', E'\t');

-- DROP FUNCTION IF EXISTS date_analysis();
CREATE OR REPLACE FUNCTION date_analysis()
    RETURNS timestamp AS
$$
BEGIN
    RETURN (SELECT MAX(analysis_formation) FROM date_of_analysis_formation);
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS days_between_dates(timestamp, timestamp) CASCADE;
CREATE OR REPLACE FUNCTION days_between_dates(late_date timestamp, early_date timestamp)
    RETURNS numeric AS
$$
DECLARE
    some_interval interval := late_date - early_date;
BEGIN
    RETURN ABS(DATE_PART('day', some_interval) + DATE_PART('hour', some_interval) / 24 +
               DATE_PART('minute', some_interval) / (24 * 60)
        + DATE_PART('second', some_interval) / (24 * 60 * 60));
END;
$$ LANGUAGE plpgsql;

-- DROP MATERIALIZED VIEW IF EXISTS v_customers CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS v_customers
AS
WITH data AS (SELECT c.customer_id,
                     AVG(t.transaction_summ)                                          AS Customer_Average_Check,
                     days_between_dates(MAX(t.transaction_datetime), MIN(t.transaction_datetime)) /
                     COUNT(*)                                                         AS Customer_Frequency,
                     days_between_dates(date_analysis(), MAX(t.transaction_datetime)) AS Customer_Inactive_Period,
                     days_between_dates(date_analysis(), MAX(t.transaction_datetime)) /
                     (days_between_dates(MAX(t.transaction_datetime), MIN(t.transaction_datetime)) /
                      COUNT(*))                                                       AS Customer_Churn_Rate
              FROM personal_data AS pd
                       JOIN cards AS c ON c.customer_id = pd.customer_id
                       JOIN transactions AS t ON t.customer_card_id = c.customer_card_id
              GROUP BY c.customer_id),
     store_transaction AS (SELECT c.customer_id,
                                  t.transaction_id,
                                  t.transaction_store_id,
                                  t.transaction_datetime
                           FROM transactions AS t
                                    JOIN cards AS c ON c.customer_card_id = t.customer_card_id
                           WHERE t.transaction_datetime <= date_analysis()),
     total_transaction AS (SELECT customer_id,
                                  COUNT(transaction_id) AS total_t
                           FROM store_transaction
                           GROUP BY customer_id),
     each_transaction AS (SELECT customer_id,
                                 transaction_store_id,
                                 COUNT(transaction_store_id) AS each_t,
                                 MAX(transaction_datetime)   AS late_day
                          FROM store_transaction
                          GROUP BY customer_id, transaction_store_id),
     share_transactions AS (SELECT et.customer_id,
                                   et.transaction_store_id,
                                   et.each_t,
                                   et.late_day,
                                   (each_t::float / tt.total_t) AS share
                            FROM total_transaction AS tt
                                     JOIN each_transaction AS et ON et.customer_id = tt.customer_id
                            ORDER BY et.customer_id, et.each_t DESC),
     rank_share AS (SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY share DESC, late_day DESC) AS rank
                    FROM share_transactions),
     max_share AS (SELECT customer_id, MAX(share) AS max_share_
                   FROM share_transactions
                   GROUP BY customer_id),
     count_share AS (SELECT sht.customer_id, share, COUNT(share) AS count
                     FROM share_transactions AS sht
                     GROUP BY sht.customer_id, share),
     count_max_share AS (SELECT csh.customer_id, count
                         FROM count_share AS csh
                                  JOIN max_share AS msh
                                       ON msh.max_share_ = csh.share AND msh.customer_id = csh.customer_id
                         ORDER BY csh.customer_id),
     count_max_id AS (SELECT sht.customer_id, transaction_store_id
                      FROM share_transactions AS sht
                               JOIN count_max_share AS cms ON cms.customer_id = sht.customer_id
                               JOIN max_share AS msh
                                    ON msh.max_share_ = sht.share AND msh.customer_id = sht.customer_id
                      WHERE count = 1),
     rank_stores AS (SELECT customer_id,
                            transaction_store_id,
                            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY transaction_datetime DESC) AS rank_3
                     FROM store_transaction),
     three_stores AS (SELECT *, COUNT(transaction_store_id) AS count
                      FROM rank_stores
                      WHERE rank_3 <= 3
                      GROUP BY customer_id, transaction_store_id, rank_3
                      order by 1),
     primary_store AS (SELECT DISTINCT rsh.customer_id,
                                       (CASE
                                            WHEN ts.count = 3
                                                THEN ts.transaction_store_id
                                            WHEN cms.count = 1
                                                THEN cmi.transaction_store_id
                                            ELSE rsh.transaction_store_id
                                           END) AS Customer_Primary_Store
                       FROM rank_share AS rsh
                                LEFT JOIN three_stores AS ts ON ts.customer_id = rsh.customer_id
                                LEFT JOIN count_max_share AS cms ON cms.customer_id = ts.customer_id
                                LEFT JOIN count_max_id AS cmi ON cmi.customer_id = ts.customer_id
                       WHERE rsh.rank = 1),
     finally AS (SELECT data.customer_id,
                        Customer_Average_Check,
                        Customer_Frequency,
                        Customer_Inactive_Period,
                        Customer_Churn_Rate,
                        CASE
                            WHEN (PERCENT_RANK() OVER (ORDER BY Customer_Average_Check DESC) <= 0.1) THEN 'High'
                            WHEN (PERCENT_RANK() OVER (ORDER BY Customer_Average_Check DESC) > 0.1) AND
                                 (PERCENT_RANK() OVER (ORDER BY Customer_Average_Check DESC) <= 0.35)
                                THEN 'Medium'
                            ELSE 'Low' END    AS Customer_Average_Check_Segment,
                        CASE
                            WHEN (PERCENT_RANK() OVER (ORDER BY Customer_Frequency) <= 0.1) THEN 'Often'
                            WHEN (PERCENT_RANK() OVER (ORDER BY Customer_Frequency) > 0.1) AND
                                 (PERCENT_RANK() OVER (ORDER BY Customer_Frequency) <= 0.35)
                                THEN 'Occasionally'
                            ELSE 'Rarely' END AS Customer_Frequency_Segment,
                        CASE
                            WHEN Customer_Churn_Rate >= 0 AND Customer_Churn_Rate <= 2 THEN 'Low'
                            WHEN Customer_Churn_Rate > 2 AND Customer_Churn_Rate <= 5 THEN 'Medium'
                            ELSE 'High' END   AS Customer_Churn_Segment,
                        Customer_Primary_Store
                 FROM data
                          JOIN primary_store AS ps ON ps.customer_id = data.customer_id)
SELECT f.customer_id AS Customer_ID,
       Customer_Average_Check,
       Customer_Average_Check_Segment,
       Customer_Frequency,
       Customer_Frequency_Segment,
       Customer_Inactive_Period,
       Customer_Churn_Rate,
       Customer_Churn_Segment,
       Segment       AS Customer_Segment,
       Customer_Primary_Store
FROM finally AS f
         LEFT JOIN segments AS s ON f.Customer_Average_Check_Segment = s.Average_check AND
                                    f.Customer_Frequency_Segment = s.Frequency_of_purchases AND
                                    f.Customer_Churn_Segment = s.Churn_probability
ORDER BY customer_id;

-- SELECT *
-- FROM v_customers;
-- SELECT *
-- FROM v_customers
-- WHERE Customer_Average_Check_Segment = 'Low';
-- SELECT *
-- FROM v_customers
-- WHERE Customer_Average_Check < 2000;
-- SELECT *
-- FROM v_customers
-- WHERE Customer_Frequency > 500
--   AND Customer_Inactive_Period > 100;
-- SELECT *
-- FROM v_customers
-- WHERE Customer_Average_Check_Segment = 'Low'
--   AND Customer_Frequency_Segment = 'Often'
--   AND Customer_Churn_Segment = 'High';
-- SELECT *
-- FROM v_customers
-- WHERE Customer_Segment >= 20;
-- SELECT *
-- FROM v_customers
-- WHERE Customer_Primary_Store = 1
--   AND Customer_ID < 10;

-- 2.2 Purchase history
-- DROP MATERIALIZED VIEW IF EXISTS v_history CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS v_history AS
SELECT DISTINCT p.customer_id,
                t.transaction_id,
                t.transaction_datetime,
                sku.group_id,
                SUM(ch.sku_amount * s.sku_purchase_price)
                OVER (PARTITION BY p.customer_id, t.transaction_id, t.transaction_datetime, sku.group_id) as group_cost,
                SUM(ch.sku_summ)
                OVER (PARTITION BY p.customer_id, t.transaction_id, t.transaction_datetime, sku.group_id) as group_summ,
                SUM(ch.sku_summ_paid)
                OVER (PARTITION BY p.customer_id, t.transaction_id, t.transaction_datetime, sku.group_id) as group_summ_paid
FROM personal_data AS p
         JOIN cards AS c ON p.customer_id = c.customer_id
         JOIN transactions AS t ON c.customer_card_id = t.customer_card_id AND
                                   t.transaction_datetime <= date_analysis()
         JOIN checks AS ch ON t.transaction_id = ch.transaction_id
         JOIN sku ON sku.sku_id = ch.sku_id
         JOIN stores AS s ON sku.sku_id = s.sku_id AND t.transaction_store_id = s.transaction_store_id
ORDER BY customer_id, group_id;

-- SELECT *
-- FROM v_history;
-- SELECT *
-- FROM v_history
-- WHERE customer_id = 14
--   AND group_id > 3;
-- SELECT *
-- FROM v_history
-- WHERE transaction_datetime BETWEEN '2019-06-29' AND '2020-01-22';
-- SELECT *
-- FROM v_history
-- WHERE group_cost > 500
--   AND group_summ < 600;
-- SELECT *
-- FROM v_history
-- WHERE group_cost > 3000
--   AND group_summ_paid < 4000;

-- 2.3 Periods
-- DROP FUNCTION IF EXISTS group_min_discount(int, int);
CREATE OR REPLACE FUNCTION group_min_discount(md_customer_id int, md_group_id int)
    RETURNS numeric
AS
$$
BEGIN
    RETURN (SELECT COALESCE(MIN(ch.sku_discount / ch.sku_summ), 0)
            FROM personal_data AS pd
                     JOIN cards AS c ON pd.customer_id = c.customer_id AND pd.customer_id = md_customer_id
                     JOIN transactions AS t ON c.customer_card_id = t.customer_card_id
                     JOIN checks AS ch ON t.transaction_id = ch.transaction_id AND ch.sku_discount > 0
                     JOIN sku AS s ON s.sku_id = ch.sku_id AND s.group_id = md_group_id);
END;
$$ LANGUAGE plpgsql;

-- DROP MATERIALIZED VIEW IF EXISTS v_periods CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS v_periods AS
SELECT pd.customer_id                               AS customer_id,
       group_id                                     AS group_id,
       MIN(transaction_datetime)                    AS first_group_purchase_date,
       MAX(transaction_datetime)                    AS last_group_purchase_date,
       COUNT(DISTINCT t.transaction_id) + 0.0       AS group_purchase,
       (days_between_dates(MAX(transaction_datetime), MIN(transaction_datetime)) + 1)
           / COUNT(DISTINCT t.transaction_id)       AS Group_Frequency,
       group_min_discount(pd.customer_id, group_id) AS Group_Min_Discount
FROM personal_data AS pd
         JOIN cards AS c ON pd.customer_id = c.customer_id
         JOIN transactions AS t ON c.customer_card_id = t.customer_card_id
         JOIN checks AS ch ON t.transaction_id = ch.transaction_id
         JOIN sku AS s ON ch.sku_id = s.sku_id
GROUP BY pd.customer_id, group_id;

-- SELECT *
-- FROM v_periods;
-- SELECT *
-- FROM v_periods
-- WHERE customer_id = 15
--   AND group_id = 2;
-- SELECT *
-- FROM v_periods
-- WHERE first_group_purchase_date > '2019-08-27'
--   AND last_group_purchase_date < '2020-04-28';
-- SELECT *
-- FROM v_periods
-- WHERE group_purchase > 6
--   AND group_frequency > 150;
-- SELECT *
-- FROM v_periods
-- WHERE group_purchase < 4
--   AND group_min_discount > 0.3;

-- 2.4 Groups
-- DROP FUNCTION IF EXISTS f_v_group(integer, integer, integer) CASCADE;
CREATE OR REPLACE FUNCTION f_v_group(int DEFAULT 1, int DEFAULT 2000, int DEFAULT 10000)
    RETURNS TABLE
            (
                Customer_ID            int,
                Group_ID               int,
                Group_Affinity_Index   numeric,
                Group_Churn_Rate       numeric,
                Group_Stability_Index  numeric,
                Group_Margin           numeric,
                Group_Discount_Share   numeric,
                Group_Minimum_Discount numeric,
                Group_Average_Discount numeric
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH group_Affinity_Index AS (SELECT vp.customer_id,
                                             vp.group_id,
                                             vp.group_min_discount                   AS Group_Minimum_Discount,
                                             (SELECT vp.group_purchase / COUNT(DISTINCT vh.transaction_id)
                                              FROM v_history AS vh
                                              WHERE vh.transaction_datetime BETWEEN vp.first_group_purchase_date AND vp.last_group_purchase_date
                                                AND vh.customer_id = vp.customer_id) AS Group_Affinity_Index
                                      FROM v_periods AS vp)
           , group_Churn_Rate AS (SELECT DISTINCT vp.customer_id,
                                                  vp.group_id,
                                                  days_between_dates(date_analysis(),
                                                                     vp.last_group_purchase_date) /
                                                  vp.group_frequency                           AS Group_Churn_Rate,
                                                  (WITH calc_interval AS (SELECT coalesce(
                                                                                         days_between_dates(
                                                                                                 vh.transaction_datetime,
                                                                                                 LAG(vh.transaction_datetime) OVER (ORDER BY vh.transaction_datetime)),
                                                                                         0) AS interval
                                                                          FROM v_history AS vh
                                                                          WHERE vh.customer_id = vp.customer_id
                                                                            AND vh.group_id = vp.group_id)
                                                   SELECT COALESCE(
                                                                  AVG(ABS(interval - vp.group_frequency) / vp.group_frequency),
                                                                  0)
                                                   FROM calc_interval
                                                   WHERE interval > 0)                         AS Stability_Index,
                                                  SUM(vh.group_summ_paid) / SUM(vh.group_summ) AS Group_Average_Discount
                                  FROM v_history AS vh
                                           JOIN v_periods AS vp
                                                on vh.customer_id = vp.customer_id AND vh.group_id = vp.group_id
                                  GROUP BY vp.customer_id, vp.group_id, vp.last_group_purchase_date, vp.group_frequency)
           , group_Margin_1 AS (SELECT SGM.customer_id, SGM.group_id, SUM(GM) AS Group_Margin
                                FROM (SELECT vh.customer_id, vh.group_id, vh.group_summ_paid - vh.group_cost AS GM
                                      FROM v_history AS vh
                                      WHERE transaction_datetime BETWEEN
                                                    (SELECT analysis_formation FROM date_of_analysis_formation) -
                                                    (INTERVAL '1 day') * $2 AND
                                                    (SELECT analysis_formation FROM date_of_analysis_formation)) AS SGM
                                GROUP BY SGM.customer_id, SGM.group_id)
           , group_Margin_2 AS (SELECT SGM.customer_id, SGM.group_id, SUM(GM) AS Group_Margin
                                FROM (SELECT vh.customer_id, vh.group_id, vh.group_summ_paid - vh.group_cost AS GM
                                      FROM v_history AS vh
                                      ORDER BY vh.transaction_datetime DESC
                                      LIMIT $3) AS SGM
                                GROUP BY SGM.customer_id, SGM.group_id)
           , group_Margin AS (SELECT gm1.customer_id,
                                     gm2.group_id,
                                     CASE
                                         WHEN ($1 = 1) THEN gm1.Group_Margin
                                         WHEN ($1 = 2) THEN gm2.Group_Margin END AS Group_Margin
                              FROM group_Margin_1 AS gm1
                                       JOIN group_Margin_2 AS gm2
                                            ON gm1.customer_id = gm2.customer_id AND gm1.group_id = gm2.group_id)
           , group_discount_share AS (SELECT p.customer_id,
                                             p.group_id,
                                             (SELECT COUNT(distinct t.transaction_id)
                                              FROM transactions AS t
                                                       JOIN cards AS c1 ON t.customer_card_id = c1.customer_card_id
                                                       JOIN checks AS c2 ON t.transaction_id = c2.transaction_id
                                                       JOIN sku AS s on c2.sku_id = s.sku_id
                                              WHERE sku_discount > 0
                                                AND c1.customer_id = p.customer_id
                                                AND s.group_id = p.group_id) / group_purchase AS Group_Discount_Share
                                      FROM v_periods AS p)
        SELECT gai.customer_id                       AS Customer_ID,
               gai.group_id                          AS Group_ID,
               gai.Group_Affinity_Index,
               gcr.Group_Churn_Rate,
               coalesce(AVG(gcr.stability_Index), 0) AS Group_Stability_Index,
               coalesce(SUM(gm.Group_Margin), 0)     AS Group_Margin,
               gds.Group_Discount_Share,
               gai.Group_Minimum_Discount,
               gcr.Group_Average_Discount
        FROM group_Affinity_Index AS gai
                 JOIN group_Churn_Rate AS gcr ON gcr.Group_ID = gai.Group_ID AND gcr.Customer_ID = gai.Customer_ID
                 JOIN group_discount_share AS gds
                      ON gds.Group_ID = gai.Group_ID AND gds.Customer_ID = gai.Customer_ID
                 JOIN group_Margin AS gm ON gm.group_id = gai.Group_ID AND gm.customer_id = gai.Customer_ID
        GROUP BY gai.Customer_ID, gai.Group_ID, gai.Group_Affinity_Index, gcr.Group_Churn_Rate,
                 gds.Group_Discount_Share, gai.Group_Minimum_Discount, gcr.Group_Average_Discount;
END
$$ LANGUAGE plpgsql;

-- DROP MATERIALIZED VIEW IF EXISTS v_group CASCADE;
CREATE MATERIALIZED VIEW v_group AS
SELECT *
FROM f_v_group();

-- SELECT *
-- FROM v_group;
-- SELECT *
-- FROM v_group
-- WHERE customer_id = 1
--   AND group_id = 7;
-- SELECT *
-- FROM v_group
-- WHERE customer_id = 3
--   AND group_id = 1;
-- SELECT *
-- FROM v_group
-- WHERE group_affinity_index >= 1
--   AND group_churn_rate < 1;
-- SELECT *
-- FROM v_group
-- WHERE group_stability_index < 0.9
--   AND group_average_discount > 0.9;
-- SELECT *
-- FROM v_group
-- WHERE group_margin > 100
--   AND group_discount_share > 0.7
--   AND group_minimum_discount > 0.05;