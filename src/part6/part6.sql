-- DROP FUNCTION IF EXISTS fnc_offers_cross_selling(int, numeric, numeric, numeric, numeric) CASCADE;
CREATE OR REPLACE FUNCTION fnc_offers_cross_selling(number_of_groups int DEFAULT 100,
                                                    maximum_churn_index numeric DEFAULT 2,
                                                    maximum_stability_index numeric DEFAULT 300,
                                                    maximum_SKU_share numeric DEFAULT 100,
                                                    allowable_margin_share numeric DEFAULT 40)
    RETURNS table
            (
                Customer_ID          int,
                SKU_Name             varchar,
                Offer_Discount_Depth numeric
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH group_selection AS (SELECT vg.customer_id,
                                        vg.group_id,
                                        (vg.group_minimum_discount * 100)::int / 5 * 5 + 5                       AS group_minimum_discount,
                                        ROW_NUMBER()
                                        OVER (PARTITION BY vg.customer_id ORDER BY vg.group_affinity_index DESC) AS rank_affinity
                                 FROM v_group AS vg
                                 WHERE vg.group_churn_rate <= maximum_churn_index
                                   AND vg.group_stability_index * 100 < maximum_stability_index),
             def_margin AS (SELECT gs.customer_id,
                                   gs.group_id,
                                   gs.group_minimum_discount,
                                   ROW_NUMBER() OVER (PARTITION BY gs.customer_id, gs.group_id
                                       ORDER BY s.sku_retail_price - s.sku_purchase_price DESC) AS max_margin,
                                   s.sku_id,
                                   vc.customer_primary_store
                            FROM group_selection AS gs
                                     JOIN v_customers AS vc ON gs.rank_affinity <= number_of_groups
                                AND gs.customer_id = vc.customer_id
                                     JOIN stores AS s ON vc.customer_primary_store = s.transaction_store_id
                                     JOIN sku ON gs.group_id = sku.group_id
                                AND s.sku_id = sku.sku_id),
             sku_max_margin AS (SELECT dm.customer_id,
                                       dm.group_id,
                                       dm.sku_id,
                                       dm.group_minimum_discount,
                                       dm.customer_primary_store
                                FROM def_margin AS dm
                                WHERE max_margin = 1),
             def_sku_share AS (SELECT *,
                                      (SELECT count(DISTINCT ch.transaction_id)
                                       FROM v_history AS vh
                                                JOIN checks AS ch ON vh.customer_id = smm.customer_id
                                           AND vh.group_id = smm.group_id
                                           AND vh.transaction_id = ch.transaction_id
                                           AND ch.sku_id = smm.sku_id)::numeric
                                          / (SELECT vp.group_purchase
                                             FROM v_periods AS vp
                                             WHERE vp.customer_id = smm.customer_id
                                               AND vp.group_id = smm.group_id) AS sku_share
                               FROM sku_max_margin AS smm),
             def_allowable_discount AS (SELECT *,
                                               (SELECT sum(st.sku_retail_price - st.sku_purchase_price) /
                                                       sum(st.sku_retail_price) * allowable_margin_share
                                                FROM stores AS st
                                                WHERE st.transaction_store_id = dss.customer_primary_store) AS allowable_discount
                                        FROM def_sku_share AS dss
                                        WHERE dss.sku_share * 100 <= maximum_SKU_share),
             discount_calc AS (SELECT dad.customer_id,
                                      s.sku_name,
                                      dad.group_minimum_discount::numeric
                               FROM def_allowable_discount AS dad
                                        JOIN sku AS s ON dad.sku_id = s.sku_id
                               WHERE dad.group_minimum_discount <= dad.allowable_discount)
        SELECT *
        FROM discount_calc AS dc;
END
$$ LANGUAGE plpgsql;

-- SELECT *
-- FROM fnc_offers_cross_selling();
SELECT *
FROM fnc_offers_cross_selling(5, 3, 50, 100, 30);
SELECT *
FROM fnc_offers_cross_selling(5, 3, 50, 100, 50);
-- SELECT *
-- FROM fnc_offers_cross_selling(20, 10, 50, 200, 100);
-- SELECT *
-- FROM fnc_offers_cross_selling(2, 80, 100, 120, 30);