
CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_fact_income_summary()
LANGUAGE plpgsql
AS $$
BEGIN

CALL finance_etl.sp_populate_source_table_fact_income_summary();

INSERT INTO finance_dw.fact_income_summary (branch_key, gl_account_id_key, corporation_key, posting_date_key, debit_amount, credit_amount, amount)
SELECT
    source.branch_key,
    source.gl_account_id_key,
    source.corporation_key,
    source.posting_date_key,
    source.debit_amount,
    source.credit_amount,
    source.amount
FROM finance_etl.fact_income_summary_source AS source
LEFT OUTER JOIN
    finance_dw.fact_income_summary AS target
    ON
        (source.branch_key = target.branch_key OR (source.branch_key IS NULL AND target.branch_key IS NULL))
        AND (source.gl_account_id_key = target.gl_account_id_key OR (source.gl_account_id_key IS NULL AND target.gl_account_id_key IS NULL))
        AND (source.corporation_key = target.corporation_key OR (source.corporation_key IS NULL AND target.corporation_key IS NULL))
        AND (source.posting_date_key = target.posting_date_key OR (source.posting_date_key IS NULL AND target.posting_date_key IS NULL))
WHERE target.fact_income_summary_key IS NULL
; 


UPDATE finance_dw.fact_income_summary SET debit_amount = update_old_rows.debit_amount, credit_amount = update_old_rows.credit_amount, amount = update_old_rows.amount FROM (
    SELECT
        target.fact_income_summary_key,
        source.branch_key,
        source.gl_account_id_key,
        source.corporation_key,
        source.posting_date_key,
        source.debit_amount,
        source.credit_amount,
        source.amount
    FROM finance_dw.fact_income_summary, finance_dw.fact_income_summary AS target
    INNER JOIN
        finance_etl.fact_income_summary_source AS source
        ON
            (target.branch_key = source.branch_key OR (source.branch_key IS NULL AND target.branch_key IS NULL))
            AND (target.gl_account_id_key = source.gl_account_id_key OR (source.gl_account_id_key IS NULL AND target.gl_account_id_key IS NULL))
            AND (target.corporation_key = source.corporation_key OR (source.corporation_key IS NULL AND target.corporation_key IS NULL))
            AND (target.posting_date_key = source.posting_date_key OR (source.posting_date_key IS NULL AND target.posting_date_key IS NULL))
    WHERE
        coalesce(finance_dw.fact_income_summary.debit_amount, 0.0) != coalesce(source.debit_amount, 0.0)
        OR coalesce(finance_dw.fact_income_summary.credit_amount, 0.0) != coalesce(source.credit_amount, 0.0)
        OR coalesce(finance_dw.fact_income_summary.amount, 0.0) != coalesce(source.amount, 0.0)
) AS update_old_rows
WHERE finance_dw.fact_income_summary.fact_income_summary_key = update_old_rows.fact_income_summary_key
; 


DELETE FROM finance_dw.fact_income_summary USING (SELECT DISTINCT target.fact_income_summary_key AS fact_income_summary_key 
FROM finance_dw.fact_income_summary AS target LEFT OUTER JOIN finance_etl.fact_income_summary_source AS source ON (source.branch_key = target.branch_key OR (source.branch_key IS NULL AND target.branch_key IS NULL)) AND (source.gl_account_id_key = target.gl_account_id_key OR (source.gl_account_id_key IS NULL AND target.gl_account_id_key IS NULL)) AND (source.corporation_key = target.corporation_key OR (source.corporation_key IS NULL AND target.corporation_key IS NULL)) AND (source.posting_date_key = target.posting_date_key OR (source.posting_date_key IS NULL AND target.posting_date_key IS NULL)) 
WHERE source.fact_income_summary_key IS NULL) AS soft_delete_subquery WHERE finance_dw.fact_income_summary.fact_income_summary_key = soft_delete_subquery.fact_income_summary_key
;
END;
$$
