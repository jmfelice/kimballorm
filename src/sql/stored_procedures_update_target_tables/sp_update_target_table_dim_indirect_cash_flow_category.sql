CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_dim_indirect_cash_flow_category()
LANGUAGE plpgsql
AS $$
BEGIN

CALL finance_etl.sp_update_source_table_dim_indirect_cash_flow_category();

UPDATE finance_dw.dim_indirect_cash_flow_category SET
    parent_indirect_cash_flow_category = update_old_rows.parent_indirect_cash_flow_category,
    indirect_cash_flow_category_order = update_old_rows.indirect_cash_flow_category_order,
    isleaf = update_old_rows.isleaf,
    level = update_old_rows.level,
    active = update_old_rows.active
FROM (
    SELECT
        target.indirect_cash_flow_category_key,
        source.indirect_cash_flow_category,
        source.parent_indirect_cash_flow_category,
        source.indirect_cash_flow_category_order,
        source.isleaf,
        source.level,
        source.active
    FROM finance_dw.dim_indirect_cash_flow_category AS target
    INNER JOIN
        finance_etl.dim_indirect_cash_flow_category_source AS source
        ON (target.indirect_cash_flow_category = source.indirect_cash_flow_category OR (source.indirect_cash_flow_category IS NULL AND target.indirect_cash_flow_category IS NULL))
    WHERE
        coalesce(target.parent_indirect_cash_flow_category, '') != coalesce(source.parent_indirect_cash_flow_category, '')
        OR coalesce(target.indirect_cash_flow_category_order, 0) != coalesce(source.indirect_cash_flow_category_order, 0)
        OR coalesce(target.isleaf, 0) != coalesce(source.isleaf, 0)
        OR coalesce(target.level, 0) != coalesce(source.level, 0)
        OR coalesce(target.active, 0) != coalesce(source.active, 0)
) AS update_old_rows
WHERE finance_dw.dim_indirect_cash_flow_category.indirect_cash_flow_category_key = update_old_rows.indirect_cash_flow_category_key
;


INSERT INTO finance_dw.dim_indirect_cash_flow_category (
    indirect_cash_flow_category_key, indirect_cash_flow_category, parent_indirect_cash_flow_category, indirect_cash_flow_category_order, isleaf, level, active
)
SELECT
    source.indirect_cash_flow_category_key,
    source.indirect_cash_flow_category,
    source.parent_indirect_cash_flow_category,
    source.indirect_cash_flow_category_order,
    source.isleaf,
    source.level,
    source.active
FROM finance_etl.dim_indirect_cash_flow_category_source AS source
LEFT OUTER JOIN
    finance_dw.dim_indirect_cash_flow_category AS target
    ON (source.indirect_cash_flow_category = target.indirect_cash_flow_category OR (source.indirect_cash_flow_category IS NULL AND target.indirect_cash_flow_category IS NULL))
WHERE target.indirect_cash_flow_category_key IS NULL
;


WITH soft_delete_cte AS (
    SELECT target.indirect_cash_flow_category_key
    FROM finance_dw.dim_indirect_cash_flow_category AS target
    LEFT OUTER JOIN
        finance_etl.dim_indirect_cash_flow_category_source AS source
        ON (target.indirect_cash_flow_category = source.indirect_cash_flow_category OR (source.indirect_cash_flow_category IS NULL AND target.indirect_cash_flow_category IS NULL))
    WHERE source.indirect_cash_flow_category_key IS NULL AND target.active = 1
)

UPDATE finance_dw.dim_indirect_cash_flow_category SET active = 0
FROM soft_delete_cte
WHERE finance_dw.dim_indirect_cash_flow_category.indirect_cash_flow_category_key = soft_delete_cte.indirect_cash_flow_category_key

;
END;
$$
