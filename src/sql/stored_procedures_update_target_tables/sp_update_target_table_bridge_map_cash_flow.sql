CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_bridge_map_cash_flow()
LANGUAGE plpgsql
AS $$
BEGIN

CALL finance_etl.sp_update_source_table_bridge_map_cash_flow();

INSERT INTO finance_dw.bridge_map_cash_flow (gl_account_id_key, indirect_cash_flow_category_key, reverse)
SELECT
    source.gl_account_id_key,
    source.indirect_cash_flow_category_key,
    source.reverse
FROM finance_etl.bridge_map_cash_flow_source AS source
LEFT OUTER JOIN
    finance_dw.bridge_map_cash_flow AS target
    ON
        (source.gl_account_id_key = target.gl_account_id_key OR (source.gl_account_id_key IS NULL AND target.gl_account_id_key IS NULL))
        AND (
            source.indirect_cash_flow_category_key = target.indirect_cash_flow_category_key
            OR (source.indirect_cash_flow_category_key IS NULL AND target.indirect_cash_flow_category_key IS NULL)
        )
WHERE target.bridge_map_cash_flow_key IS NULL
;


UPDATE finance_dw.bridge_map_cash_flow SET reverse = update_old_rows.reverse FROM (
    SELECT
        target.bridge_map_cash_flow_key,
        source.gl_account_id_key,
        source.indirect_cash_flow_category_key,
        source.reverse
    FROM finance_dw.bridge_map_cash_flow, finance_dw.bridge_map_cash_flow AS target
    INNER JOIN
        finance_etl.bridge_map_cash_flow_source AS source
        ON
            (target.gl_account_id_key = source.gl_account_id_key OR (source.gl_account_id_key IS NULL AND target.gl_account_id_key IS NULL))
            AND (
                target.indirect_cash_flow_category_key = source.indirect_cash_flow_category_key
                OR (source.indirect_cash_flow_category_key IS NULL AND target.indirect_cash_flow_category_key IS NULL)
            )
    WHERE coalesce(finance_dw.bridge_map_cash_flow.reverse, 0) != coalesce(source.reverse, 0)
) AS update_old_rows
WHERE finance_dw.bridge_map_cash_flow.bridge_map_cash_flow_key = update_old_rows.bridge_map_cash_flow_key
;


DELETE FROM finance_dw.bridge_map_cash_flow USING (SELECT DISTINCT target.bridge_map_cash_flow_key AS bridge_map_cash_flow_key
FROM finance_dw.bridge_map_cash_flow AS target LEFT OUTER JOIN finance_etl.bridge_map_cash_flow_source AS source ON (source.gl_account_id_key = target.gl_account_id_key OR (source.gl_account_id_key IS NULL AND target.gl_account_id_key IS NULL)) AND (source.indirect_cash_flow_category_key = target.indirect_cash_flow_category_key OR (source.indirect_cash_flow_category_key IS NULL AND target.indirect_cash_flow_category_key IS NULL))
WHERE source.bridge_map_cash_flow_key IS NULL) AS soft_delete_subquery WHERE finance_dw.bridge_map_cash_flow.bridge_map_cash_flow_key = soft_delete_subquery.bridge_map_cash_flow_key
;
END;
$$
