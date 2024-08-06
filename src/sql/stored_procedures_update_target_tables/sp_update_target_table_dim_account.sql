CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_dim_account()
LANGUAGE plpgsql
AS $$
BEGIN

CALL finance_etl.sp_update_source_table_dim_account();

UPDATE finance_dw.dim_account SET
    gl_account_description = update_old_rows.gl_account_description,
    account_class = update_old_rows.account_class,
    gl_category = update_old_rows.gl_category,
    intercompany_flag = update_old_rows.intercompany_flag,
    active = update_old_rows.active
FROM (
    SELECT
        target.gl_account_id_key,
        source.gl_account_id,
        source.gl_account_description,
        source.account_class,
        source.gl_category,
        source.intercompany_flag,
        source.active
    FROM finance_dw.dim_account AS target
    INNER JOIN finance_etl.dim_account_source AS source ON (target.gl_account_id = source.gl_account_id OR (source.gl_account_id IS NULL AND target.gl_account_id IS NULL))
    WHERE
        coalesce(target.gl_account_description, '') != coalesce(source.gl_account_description, '')
        OR coalesce(target.account_class, '') != coalesce(source.account_class, '')
        OR coalesce(target.gl_category, '') != coalesce(source.gl_category, '')
        OR coalesce(target.intercompany_flag, 0) != coalesce(source.intercompany_flag, 0)
        OR coalesce(target.active, 0) != coalesce(source.active, 0)
) AS update_old_rows
WHERE finance_dw.dim_account.gl_account_id_key = update_old_rows.gl_account_id_key
;


INSERT INTO finance_dw.dim_account (gl_account_id_key, gl_account_id, gl_account_description, account_class, gl_category, intercompany_flag, active)
SELECT
    source.gl_account_id_key,
    source.gl_account_id,
    source.gl_account_description,
    source.account_class,
    source.gl_category,
    source.intercompany_flag,
    source.active
FROM finance_etl.dim_account_source AS source
LEFT OUTER JOIN finance_dw.dim_account AS target ON (source.gl_account_id = target.gl_account_id OR (source.gl_account_id IS NULL AND target.gl_account_id IS NULL))
WHERE target.gl_account_id_key IS NULL
;


WITH soft_delete_cte AS (
    SELECT target.gl_account_id_key
    FROM finance_dw.dim_account AS target
    LEFT OUTER JOIN finance_etl.dim_account_source AS source ON (target.gl_account_id = source.gl_account_id OR (source.gl_account_id IS NULL AND target.gl_account_id IS NULL))
    WHERE source.gl_account_id_key IS NULL AND target.active = 1
)

UPDATE finance_dw.dim_account SET active = 0 FROM soft_delete_cte WHERE finance_dw.dim_account.gl_account_id_key = soft_delete_cte.gl_account_id_key

;
END;
$$
