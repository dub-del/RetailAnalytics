-- DROP FUNCTION min_date_transaction() CASCADE;
CREATE OR REPLACE FUNCTION min_date_transaction()
    RETURNS timestamp
AS
$$
BEGIN
    RETURN (SELECT MIN(transaction_datetime) FROM transactions);
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS fnc_method1(timestamp, timestamp, numeric) CASCADE;
CREATE OR REPLACE FUNCTION fnc_method1(first_date timestamp,
                                       last_date timestamp,
                                       increase_factor numeric)
    RETURNS TABLE
            (
                Customer_ID            int,
                Required_Check_Measure numeric
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT c.customer_id,
               AVG(transaction_summ) * increase_factor
        FROM transactions AS t
                 JOIN cards AS c ON c.customer_card_id = t.customer_card_id
        WHERE transaction_datetime BETWEEN first_date AND last_date
        GROUP BY c.customer_id;
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS fnc_method2(int, numeric) CASCADE;
CREATE OR REPLACE FUNCTION fnc_method2(num_transactions int, increase_factor numeric)
    RETURNS TABLE
            (
                Customer_ID            int,
                Required_Check_Measure numeric
            )
AS
$$
BEGIN
    RETURN QUERY (WITH rank AS (SELECT c.customer_id,
                                       transaction_summ,
                                       ROW_NUMBER()
                                       OVER (PARTITION BY c.customer_id ORDER BY transaction_datetime DESC) AS rank_trans
                                FROM transactions AS t
                                         JOIN cards AS c ON c.customer_card_id = t.customer_card_id)
                  SELECT r.customer_id, AVG(transaction_summ) * increase_factor
                  FROM rank AS r
                  WHERE rank_trans <= num_transactions
                  GROUP BY r.customer_id);
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS fnc_offer_condition_determination(int, timestamp, timestamp, int, numeric) CASCADE;
CREATE OR REPLACE FUNCTION fnc_offer_condition_determination(calc_method int,
                                                             first_date timestamp,
                                                             last_date timestamp,
                                                             num_transactions int,
                                                             increase_factor numeric)
    RETURNS TABLE
            (
                Customer_ID            int,
                Required_Check_Measure numeric
            )
AS
$$
BEGIN
    IF increase_factor ISNULL THEN
        RAISE EXCEPTION 'ERROR: You must specify a factor to increase the average check
                         to determine the target value of the average check';
    END IF;
    IF (calc_method = 1) THEN
        IF first_date >= last_date OR first_date ISNULL THEN
            first_date := min_date_transaction();
        END IF;
        IF last_date > date_analysis() OR last_date ISNULL THEN
            last_date := date_analysis();
        END IF;
        RETURN QUERY (SELECT m1.Customer_ID, m1.Required_Check_Measure
                      FROM fnc_method1(first_date, last_date, increase_factor) m1);
    ELSEIF (calc_method = 2) THEN
        IF num_transactions ISNULL THEN
            RAISE EXCEPTION 'ERROR: You must specify the quantity
                             transactions to calculate the average check';
        END IF;
        RETURN QUERY (SELECT m2.Customer_ID, m2.Required_Check_Measure
                      FROM fnc_method2(num_transactions, increase_factor) m2);
    ELSE
        RAISE EXCEPTION 'ERROR: Method does not exist';
    END IF;
END
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS fnc_reward_determination(numeric, numeric, numeric) CASCADE;
CREATE OR REPLACE FUNCTION fnc_reward_determination(max_churn_ind numeric DEFAULT 500,
                                                    max_share_trans numeric DEFAULT 1,
                                                    share_margin numeric DEFAULT 5)
    RETURNS TABLE
            (
                Customer_ID          int,
                Group_name           varchar,
                Offer_Discount_Depth numeric
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH def_group_discount AS (SELECT DISTINCT vg.customer_id,
                                                    FIRST_VALUE(vg.group_id)
                                                    OVER (PARTITION BY vg.customer_id ORDER BY vg.group_affinity_index DESC) AS group_id,
                                                    ((FIRST_VALUE(vg.group_minimum_discount)
                                                      OVER (PARTITION BY vg.customer_id ORDER BY vg.group_affinity_index DESC) *
                                                      100)::int / 5 + 1) *
                                                    5                                                                        AS offer_discount_depth
                                    FROM v_group AS vg
                                    WHERE vg.group_churn_rate <= max_churn_ind
                                      AND vg.group_discount_share * 100 < max_share_trans
                                      AND (round(Group_Minimum_Discount*100)::int - (round(Group_Minimum_Discount*100)::int % 5) + 5)
                                        < (SELECT SUM(st.sku_retail_price - st.sku_purchase_price) /
                                                  SUM(st.sku_retail_price)
                                           FROM sku AS s
                                                    JOIN stores AS st ON vg.group_id = s.group_id
                                               AND s.sku_id = st.sku_id) * share_margin)
        SELECT dgd.customer_id,
               gs.group_name,
               dgd.offer_discount_depth::numeric
        FROM def_group_discount AS dgd
                 JOIN groups_sku AS gs ON gs.group_id = dgd.group_id
        ORDER BY dgd.customer_id;
END
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS fnc_personal_offers(int, timestamp, timestamp, int, numeric, numeric, numeric, numeric) CASCADE;
CREATE OR REPLACE FUNCTION fnc_personal_offers(calc_method int,
                                               first_date timestamp,
                                               last_date timestamp,
                                               num_transactions int,
                                               increase_factor numeric,
                                               max_churn_ind numeric,
                                               max_share_trans numeric,
                                               share_margin numeric)
    RETURNS TABLE
            (
                Customer_ID            int,
                Required_Check_Measure numeric,
                Group_Name             varchar,
                Offer_Discount_Depth   numeric
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH condition AS (SELECT c.Customer_ID, c.Required_Check_Measure
                           FROM fnc_offer_condition_determination(calc_method,
                                                                  first_date,
                                                                  last_date,
                                                                  num_transactions,
                                                                  increase_factor) AS c),
             offer AS (SELECT o.Customer_ID, o.Group_name, o.Offer_Discount_Depth
                       FROM fnc_reward_determination(max_churn_ind,
                                                     max_share_trans,
                                                     share_margin) AS o)
        SELECT c.Customer_ID, c.Required_Check_Measure, o.Group_name, o.Offer_Discount_Depth
        FROM condition AS c
                 JOIN offer AS o ON c.Customer_ID = o.Customer_ID;
END
$$ LANGUAGE plpgsql;

-- -- Проверка метода расчета за период
-- SELECT *
-- FROM fnc_personal_offers(1, '2018-03-19', '2020-01-25', NULL, 2, 5, 100, 50);
-- -- Проверка метода расчета за период, если первая дата равна последней дате
-- SELECT *
-- FROM fnc_personal_offers(1, '2018-03-19', '2018-03-19', NULL, 2, 5, 100,
--                          50);
-- -- Проверка метода расчета за период, если последняя дата позже анализируемой даты
-- SELECT *
-- FROM fnc_personal_offers(1, '2018-03-19', '2022-08-22', NULL, 2, 5, 100,
--                          50);
-- -- Проверка метода расчета за период, если отсутствует период
-- SELECT *
-- FROM fnc_personal_offers(1, NULL, NULL, NULL, 2, 5, 100, 50);
-- -- Проверка метода расчета по количеству последних транзакций
-- SELECT *
-- FROM fnc_personal_offers(2, NULL, NULL, 100, 1.15, 3, 70, 30);
-- -- Проверка метода расчета по количеству последних транзакций, если не было введено значение количества транзакций
-- SELECT *
-- FROM fnc_personal_offers(2, NULL, NULL, NULL, 2, 5, 100, 50);
-- -- Проверка метода расчета по количеству последних транзакций, если не было введено значение коэффициента увеличения среднего чека
-- SELECT *
-- FROM fnc_personal_offers(2, NULL, NULL, 45, NULL, 5, 100, 50);
-- -- Проверка несуществующего метода
-- SELECT *
-- FROM fnc_personal_offers(3, NULL, NULL, NULL, 2, 5, 100, 50);