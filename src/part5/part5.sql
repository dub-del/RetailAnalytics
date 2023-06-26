-- DROP FUNCTION IF EXISTS fnc_personal_offers_visits(timestamp, timestamp, int, numeric, numeric, numeric) CASCADE;
CREATE OR REPLACE FUNCTION fnc_personal_offers_visits(first_date timestamp DEFAULT '2018-03-18 21:17:43.000000',
                                                      last_date timestamp DEFAULT '2022-08-18 21:17:43.000000',
                                                      num_transactions int DEFAULT 1,
                                                      max_churn_ind numeric DEFAULT 50,
                                                      max_share_trans numeric DEFAULT 100,
                                                      share_margin numeric DEFAULT 30)
    RETURNS table
            (
                Customer_ID                 int,
                Start_Date                  timestamp,
                End_Date                    timestamp,
                Required_Transactions_Count numeric,
                Group_Name                  varchar,
                Offer_Discount_Depth        numeric
            )
AS
$$
BEGIN
    IF first_date > last_date THEN
        RAISE EXCEPTION 'ERROR: The start date must be earlier than the end date';
    END IF;
    RETURN QUERY
        SELECT DISTINCT vg.customer_id,
                        first_date,
                        last_date,
                        (ROUND(days_between_dates(first_date, last_date) / (SELECT customer_frequency
                                                                            FROM v_customers AS c
                                                                            WHERE c.customer_id = vg.customer_id))::int +
                         num_transactions)::numeric,
                        FIRST_VALUE(gs.group_name)
                        OVER (PARTITION BY vg.customer_id ORDER BY vg.group_affinity_index DESC),
                        ((FIRST_VALUE(vg.group_minimum_discount)
                          OVER (PARTITION BY vg.customer_id ORDER BY vg.group_affinity_index DESC) * 100)::int / 5 * 5 +
                         5)::numeric
        FROM v_group AS vg
                 JOIN groups_sku AS gs ON gs.group_id = vg.group_id
            AND vg.group_churn_rate <= max_churn_ind
            AND vg.group_discount_share * 100 < max_share_trans
            AND (vg.group_minimum_discount * 100)::int / 5 * 5 + 5
                                              <
                (SELECT SUM(st.sku_retail_price - st.sku_purchase_price) / SUM(st.sku_retail_price)
                 FROM sku AS s
                          JOIN stores AS st ON vg.group_id = s.group_id
                     AND s.sku_id = st.sku_id) * share_margin;
END
$$ LANGUAGE plpgsql;

-- -- Проверка дефолтных значений
-- SELECT *
-- FROM fnc_personal_offers_visits();
-- -- Проверка функции с определенными входными данными
-- SELECT *
-- FROM fnc_personal_offers_visits('18.08.2022', '18.08.2022', 1, 3, 70, 30);
-- SELECT *
-- FROM fnc_personal_offers_visits('26.10.2019', '04.04.2020', 10, 5, 100, 20);
-- SELECT *
-- FROM fnc_personal_offers_visits('10.10.2021', '10.08.2022', 100, 50, 200, 100);
-- -- Проверка на даты
-- SELECT *
-- FROM fnc_personal_offers_visits('19.08.2022', '18.08.2022', 1, 3, 70, 30);