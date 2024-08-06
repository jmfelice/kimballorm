CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_dim_category()
LANGUAGE plpgsql
AS $$
BEGIN

CALL finance_etl.sp_update_source_table_dim_category();

UPDATE finance_dw.dim_category SET
    parent_category = update_old_rows.parent_category,
    category_class = update_old_rows.category_class,
    category_order = update_old_rows.category_order,
    isleaf = update_old_rows.isleaf,
    level = update_old_rows.level,
    active = update_old_rows.active
FROM (
    SELECT
        target.category_key,
        source.category,
        source.parent_category,
        source.category_class,
        source.category_order,
        source.isleaf,
        source.level,
        source.active
    FROM finance_dw.dim_category AS target
    INNER JOIN finance_etl.dim_category_source AS source ON (target.category = source.category OR (source.category IS NULL AND target.category IS NULL))
    WHERE
        coalesce(target.parent_category, '') != coalesce(source.parent_category, '')
        OR coalesce(target.category_class, '') != coalesce(source.category_class, '')
        OR coalesce(target.category_order, 0) != coalesce(source.category_order, 0)
        OR coalesce(target.isleaf, 0) != coalesce(source.isleaf, 0)
        OR coalesce(target.level, 0) != coalesce(source.level, 0)
        OR coalesce(target.active, 0) != coalesce(source.active, 0)
) AS update_old_rows
WHERE finance_dw.dim_category.category_key = update_old_rows.category_key
;


INSERT INTO finance_dw.dim_category (category_key, category, parent_category, category_class, category_order, isleaf, level, active)
SELECT
    source.category_key,
    source.category,
    source.parent_category,
    source.category_class,
    source.category_order,
    source.isleaf,
    source.level,
    source.active
FROM finance_etl.dim_category_source AS source
LEFT OUTER JOIN finance_dw.dim_category AS target ON (source.category = target.category OR (source.category IS NULL AND target.category IS NULL))
WHERE target.category_key IS NULL
;


WITH soft_delete_cte AS (
    SELECT target.category_key
    FROM finance_dw.dim_category AS target
    LEFT OUTER JOIN finance_etl.dim_category_source AS source ON (target.category = source.category OR (source.category IS NULL AND target.category IS NULL))
    WHERE source.category_key IS NULL AND target.active = 1
)

UPDATE finance_dw.dim_category SET active = 0 FROM soft_delete_cte WHERE finance_dw.dim_category.category_key = soft_delete_cte.category_key

;
END;
$$
