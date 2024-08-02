
CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_dim_account_class()
LANGUAGE plpgsql
AS $$
BEGIN

CALL finance_etl.sp_populate_source_table_dim_account_class();

UPDATE finance_dw.dim_account_class SET
    account_class_description = update_old_rows.account_class_description, account_class_order = update_old_rows.account_class_order, active = update_old_rows.active
FROM (
    SELECT
        target.account_class_key,
        source.account_class,
        source.account_class_description,
        source.account_class_order,
        source.active
    FROM finance_dw.dim_account_class AS target
    INNER JOIN finance_etl.dim_account_class_source AS source ON (target.account_class = source.account_class OR (source.account_class IS NULL AND target.account_class IS NULL))
    WHERE
        coalesce(target.account_class_description, '') != coalesce(source.account_class_description, '')
        OR coalesce(target.account_class_order, 0) != coalesce(source.account_class_order, 0)
        OR coalesce(target.active, 0) != coalesce(source.active, 0)
) AS update_old_rows
WHERE finance_dw.dim_account_class.account_class_key = update_old_rows.account_class_key
; 


INSERT INTO finance_dw.dim_account_class (account_class_key, account_class, account_class_description, account_class_order, active)
SELECT
    source.account_class_key,
    source.account_class,
    source.account_class_description,
    source.account_class_order,
    source.active
FROM finance_etl.dim_account_class_source AS source
LEFT OUTER JOIN finance_dw.dim_account_class AS target ON (source.account_class = target.account_class OR (source.account_class IS NULL AND target.account_class IS NULL))
WHERE target.account_class_key IS NULL
; 


WITH soft_delete_cte AS (
    SELECT target.account_class_key
    FROM finance_dw.dim_account_class AS target
    LEFT OUTER JOIN
        finance_etl.dim_account_class_source AS source
        ON (target.account_class = source.account_class OR (source.account_class IS NULL AND target.account_class IS NULL))
    WHERE source.account_class_key IS NULL AND target.active = 1
)

UPDATE finance_dw.dim_account_class SET active = 0 FROM soft_delete_cte WHERE finance_dw.dim_account_class.account_class_key = soft_delete_cte.account_class_key

;
END;
$$
