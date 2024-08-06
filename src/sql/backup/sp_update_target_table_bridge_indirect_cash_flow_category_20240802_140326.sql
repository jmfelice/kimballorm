
CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_bridge_indirect_cash_flow_category()
LANGUAGE plpgsql
AS $$
BEGIN

CALL finance_etl.sp_update_source_table_bridge_indirect_cash_flow_category();

INSERT INTO finance_dw.bridge_indirect_cash_flow_category (
    indirect_cash_flow_category_key,
    child_indirect_cash_flow_category_key,
    indirect_cash_flow_category,
    child_indirect_cash_flow_category,
    indirect_cash_flow_category_order,
    level,
    isleaf
)
SELECT
    source.indirect_cash_flow_category_key,
    source.child_indirect_cash_flow_category_key,
    source.indirect_cash_flow_category,
    source.child_indirect_cash_flow_category,
    source.indirect_cash_flow_category_order,
    source.level,
    source.isleaf
FROM finance_etl.bridge_indirect_cash_flow_category_source AS source
LEFT OUTER JOIN
    finance_dw.bridge_indirect_cash_flow_category AS target
    ON
        (
            source.indirect_cash_flow_category_key = target.indirect_cash_flow_category_key
            OR (source.indirect_cash_flow_category_key IS NULL AND target.indirect_cash_flow_category_key IS NULL)
        )
        AND (
            source.child_indirect_cash_flow_category_key = target.child_indirect_cash_flow_category_key
            OR (source.child_indirect_cash_flow_category_key IS NULL AND target.child_indirect_cash_flow_category_key IS NULL)
        )
WHERE target.bridge_indirect_cash_flow_category_key IS NULL
;


UPDATE finance_dw.bridge_indirect_cash_flow_category SET
    indirect_cash_flow_category = update_old_rows.indirect_cash_flow_category,
    child_indirect_cash_flow_category = update_old_rows.child_indirect_cash_flow_category,
    indirect_cash_flow_category_order = update_old_rows.indirect_cash_flow_category_order,
    level = update_old_rows.level,
    isleaf = update_old_rows.isleaf
FROM (
    SELECT
        target.bridge_indirect_cash_flow_category_key,
        source.indirect_cash_flow_category_key,
        source.child_indirect_cash_flow_category_key,
        source.indirect_cash_flow_category,
        source.child_indirect_cash_flow_category,
        source.indirect_cash_flow_category_order,
        source.level,
        source.isleaf
    FROM finance_dw.bridge_indirect_cash_flow_category, finance_dw.bridge_indirect_cash_flow_category AS target
    INNER JOIN
        finance_etl.bridge_indirect_cash_flow_category_source AS source
        ON
            (
                target.indirect_cash_flow_category_key = source.indirect_cash_flow_category_key
                OR (source.indirect_cash_flow_category_key IS NULL AND target.indirect_cash_flow_category_key IS NULL)
            )
            AND (
                target.child_indirect_cash_flow_category_key = source.child_indirect_cash_flow_category_key
                OR (source.child_indirect_cash_flow_category_key IS NULL AND target.child_indirect_cash_flow_category_key IS NULL)
            )
    WHERE
        coalesce(finance_dw.bridge_indirect_cash_flow_category.indirect_cash_flow_category, '') != coalesce(source.indirect_cash_flow_category, '')
        OR coalesce(finance_dw.bridge_indirect_cash_flow_category.child_indirect_cash_flow_category, '') != coalesce(source.child_indirect_cash_flow_category, '')
        OR coalesce(finance_dw.bridge_indirect_cash_flow_category.indirect_cash_flow_category_order, 0) != coalesce(source.indirect_cash_flow_category_order, 0)
        OR coalesce(finance_dw.bridge_indirect_cash_flow_category.level, 0) != coalesce(source.level, 0)
        OR coalesce(finance_dw.bridge_indirect_cash_flow_category.isleaf, 0) != coalesce(source.isleaf, 0)
) AS update_old_rows
WHERE finance_dw.bridge_indirect_cash_flow_category.bridge_indirect_cash_flow_category_key = update_old_rows.bridge_indirect_cash_flow_category_key
;


DELETE FROM finance_dw.bridge_indirect_cash_flow_category USING SELECT DISTINCT target.bridge_indirect_cash_flow_category_key AS bridge_indirect_cash_flow_category_key
FROM finance_dw.bridge_indirect_cash_flow_category AS target LEFT OUTER JOIN finance_etl.bridge_indirect_cash_flow_category_source AS source ON (source.indirect_cash_flow_category_key = target.indirect_cash_flow_category_key OR (source.indirect_cash_flow_category_key IS NULL AND target.indirect_cash_flow_category_key IS NULL)) AND (source.child_indirect_cash_flow_category_key = target.child_indirect_cash_flow_category_key OR (source.child_indirect_cash_flow_category_key IS NULL AND target.child_indirect_cash_flow_category_key IS NULL))
WHERE source.bridge_indirect_cash_flow_category_key IS NULL WHERE finance_dw.bridge_indirect_cash_flow_category.bridge_indirect_cash_flow_category_key = soft_delete_subquery.bridge_indirect_cash_flow_category_key
;
END;
$$
