CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_bridge_category()
LANGUAGE plpgsql
AS $$
BEGIN

CALL finance_etl.sp_update_source_table_bridge_category();

INSERT INTO finance_dw.bridge_category (category_key, child_category_key, category, child_category, category_order, category_class, level, isleaf)
SELECT
    source.category_key,
    source.child_category_key,
    source.category,
    source.child_category,
    source.category_order,
    source.category_class,
    source.level,
    source.isleaf
FROM finance_etl.bridge_category_source AS source
LEFT OUTER JOIN
    finance_dw.bridge_category AS target
    ON
        (source.category_key = target.category_key OR (source.category_key IS NULL AND target.category_key IS NULL))
        AND (source.child_category_key = target.child_category_key OR (source.child_category_key IS NULL AND target.child_category_key IS NULL))
WHERE target.bridge_category_key IS NULL
;


UPDATE finance_dw.bridge_category SET
    category = update_old_rows.category,
    child_category = update_old_rows.child_category,
    category_order = update_old_rows.category_order,
    category_class = update_old_rows.category_class,
    level = update_old_rows.level,
    isleaf = update_old_rows.isleaf
FROM (
    SELECT
        target.bridge_category_key,
        source.category_key,
        source.child_category_key,
        source.category,
        source.child_category,
        source.category_order,
        source.category_class,
        source.level,
        source.isleaf
    FROM finance_dw.bridge_category, finance_dw.bridge_category AS target
    INNER JOIN
        finance_etl.bridge_category_source AS source
        ON
            (target.category_key = source.category_key OR (source.category_key IS NULL AND target.category_key IS NULL))
            AND (target.child_category_key = source.child_category_key OR (source.child_category_key IS NULL AND target.child_category_key IS NULL))
    WHERE
        coalesce(finance_dw.bridge_category.category, '') != coalesce(source.category, '')
        OR coalesce(finance_dw.bridge_category.child_category, '') != coalesce(source.child_category, '')
        OR coalesce(finance_dw.bridge_category.category_order, 0) != coalesce(source.category_order, 0)
        OR coalesce(finance_dw.bridge_category.category_class, '') != coalesce(source.category_class, '')
        OR coalesce(finance_dw.bridge_category.level, 0) != coalesce(source.level, 0)
        OR coalesce(finance_dw.bridge_category.isleaf, 0) != coalesce(source.isleaf, 0)
) AS update_old_rows
WHERE finance_dw.bridge_category.bridge_category_key = update_old_rows.bridge_category_key
;


DELETE FROM finance_dw.bridge_category USING
(
    SELECT DISTINCT
    target.bridge_category_key AS bridge_category_key
    FROM finance_dw.bridge_category AS target
    LEFT OUTER JOIN finance_etl.bridge_category_source AS source ON
    (source.category_key = target.category_key OR (source.category_key IS NULL AND target.category_key IS NULL)) AND
    (source.child_category_key = target.child_category_key OR (source.child_category_key IS NULL AND target.child_category_key IS NULL))
    WHERE source.bridge_category_key IS NULL
) AS soft_delete_subquery

WHERE finance_dw.bridge_category.bridge_category_key = soft_delete_subquery.bridge_category_key
;
END;
$$
