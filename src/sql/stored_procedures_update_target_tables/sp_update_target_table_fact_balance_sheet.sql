
CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_fact_balance_sheet()
LANGUAGE plpgsql
AS $$
BEGIN

CALL finance_etl.sp_populate_source_table_fact_balance_sheet();

INSERT INTO finance_dw.fact_balance_sheet (branch_key, gl_account_id_key, category_key, corporation_key, posting_date_key, debit_balance, credit_balance, balance)
SELECT
    source.branch_key,
    source.gl_account_id_key,
    source.category_key,
    source.corporation_key,
    source.posting_date_key,
    source.debit_balance,
    source.credit_balance,
    source.balance
FROM finance_etl.fact_balance_sheet_source AS source
LEFT OUTER JOIN
    finance_dw.fact_balance_sheet AS target
    ON
        (source.branch_key = target.branch_key OR (source.branch_key IS NULL AND target.branch_key IS NULL))
        AND (source.gl_account_id_key = target.gl_account_id_key OR (source.gl_account_id_key IS NULL AND target.gl_account_id_key IS NULL))
        AND (source.category_key = target.category_key OR (source.category_key IS NULL AND target.category_key IS NULL))
        AND (source.corporation_key = target.corporation_key OR (source.corporation_key IS NULL AND target.corporation_key IS NULL))
        AND (source.posting_date_key = target.posting_date_key OR (source.posting_date_key IS NULL AND target.posting_date_key IS NULL))
WHERE target.fact_balance_sheet_key IS NULL
; 


UPDATE finance_dw.fact_balance_sheet SET debit_balance = update_old_rows.debit_balance, credit_balance = update_old_rows.credit_balance, balance = update_old_rows.balance FROM (
    SELECT
        target.fact_balance_sheet_key,
        source.branch_key,
        source.gl_account_id_key,
        source.category_key,
        source.corporation_key,
        source.posting_date_key,
        source.debit_balance,
        source.credit_balance,
        source.balance
    FROM finance_dw.fact_balance_sheet, finance_dw.fact_balance_sheet AS target
    INNER JOIN
        finance_etl.fact_balance_sheet_source AS source
        ON
            (target.branch_key = source.branch_key OR (source.branch_key IS NULL AND target.branch_key IS NULL))
            AND (target.gl_account_id_key = source.gl_account_id_key OR (source.gl_account_id_key IS NULL AND target.gl_account_id_key IS NULL))
            AND (target.category_key = source.category_key OR (source.category_key IS NULL AND target.category_key IS NULL))
            AND (target.corporation_key = source.corporation_key OR (source.corporation_key IS NULL AND target.corporation_key IS NULL))
            AND (target.posting_date_key = source.posting_date_key OR (source.posting_date_key IS NULL AND target.posting_date_key IS NULL))
    WHERE
        coalesce(finance_dw.fact_balance_sheet.debit_balance, 0.0) != coalesce(source.debit_balance, 0.0)
        OR coalesce(finance_dw.fact_balance_sheet.credit_balance, 0.0) != coalesce(source.credit_balance, 0.0)
        OR coalesce(finance_dw.fact_balance_sheet.balance, 0.0) != coalesce(source.balance, 0.0)
) AS update_old_rows
WHERE finance_dw.fact_balance_sheet.fact_balance_sheet_key = update_old_rows.fact_balance_sheet_key
; 


DELETE FROM finance_dw.fact_balance_sheet USING (SELECT DISTINCT target.fact_balance_sheet_key AS fact_balance_sheet_key 
FROM finance_dw.fact_balance_sheet AS target LEFT OUTER JOIN finance_etl.fact_balance_sheet_source AS source ON (source.branch_key = target.branch_key OR (source.branch_key IS NULL AND target.branch_key IS NULL)) AND (source.gl_account_id_key = target.gl_account_id_key OR (source.gl_account_id_key IS NULL AND target.gl_account_id_key IS NULL)) AND (source.category_key = target.category_key OR (source.category_key IS NULL AND target.category_key IS NULL)) AND (source.corporation_key = target.corporation_key OR (source.corporation_key IS NULL AND target.corporation_key IS NULL)) AND (source.posting_date_key = target.posting_date_key OR (source.posting_date_key IS NULL AND target.posting_date_key IS NULL)) 
WHERE source.fact_balance_sheet_key IS NULL) AS soft_delete_subquery WHERE finance_dw.fact_balance_sheet.fact_balance_sheet_key = soft_delete_subquery.fact_balance_sheet_key
;
END;
$$
