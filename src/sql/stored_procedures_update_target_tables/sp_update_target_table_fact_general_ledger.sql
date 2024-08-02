
CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_fact_general_ledger()
LANGUAGE plpgsql
AS $$
BEGIN

CALL finance_etl.sp_populate_source_table_fact_general_ledger();

INSERT INTO finance_dw.fact_general_ledger (
    gl_account_id_key, branch_key, corporation_key, category_key, description_key, journal_entry_id_key, posting_date_key, debit_amount, credit_amount, amount
)
SELECT
    source.gl_account_id_key,
    source.branch_key,
    source.corporation_key,
    source.category_key,
    source.description_key,
    source.journal_entry_id_key,
    source.posting_date_key,
    source.debit_amount,
    source.credit_amount,
    source.amount
FROM finance_etl.fact_general_ledger_source AS source
LEFT OUTER JOIN
    finance_dw.fact_general_ledger AS target
    ON
        (source.gl_account_id_key = target.gl_account_id_key OR (source.gl_account_id_key IS NULL AND target.gl_account_id_key IS NULL))
        AND (source.branch_key = target.branch_key OR (source.branch_key IS NULL AND target.branch_key IS NULL))
        AND (source.corporation_key = target.corporation_key OR (source.corporation_key IS NULL AND target.corporation_key IS NULL))
        AND (source.category_key = target.category_key OR (source.category_key IS NULL AND target.category_key IS NULL))
        AND (source.description_key = target.description_key OR (source.description_key IS NULL AND target.description_key IS NULL))
        AND (source.journal_entry_id_key = target.journal_entry_id_key OR (source.journal_entry_id_key IS NULL AND target.journal_entry_id_key IS NULL))
        AND (source.posting_date_key = target.posting_date_key OR (source.posting_date_key IS NULL AND target.posting_date_key IS NULL))
WHERE target.fact_general_ledger_key IS NULL
; 


UPDATE finance_dw.fact_general_ledger SET debit_amount = update_old_rows.debit_amount, credit_amount = update_old_rows.credit_amount, amount = update_old_rows.amount FROM (
    SELECT
        target.fact_general_ledger_key,
        source.gl_account_id_key,
        source.branch_key,
        source.corporation_key,
        source.category_key,
        source.description_key,
        source.journal_entry_id_key,
        source.posting_date_key,
        source.debit_amount,
        source.credit_amount,
        source.amount
    FROM finance_dw.fact_general_ledger, finance_dw.fact_general_ledger AS target
    INNER JOIN
        finance_etl.fact_general_ledger_source AS source
        ON
            (target.gl_account_id_key = source.gl_account_id_key OR (source.gl_account_id_key IS NULL AND target.gl_account_id_key IS NULL))
            AND (target.branch_key = source.branch_key OR (source.branch_key IS NULL AND target.branch_key IS NULL))
            AND (target.corporation_key = source.corporation_key OR (source.corporation_key IS NULL AND target.corporation_key IS NULL))
            AND (target.category_key = source.category_key OR (source.category_key IS NULL AND target.category_key IS NULL))
            AND (target.description_key = source.description_key OR (source.description_key IS NULL AND target.description_key IS NULL))
            AND (target.journal_entry_id_key = source.journal_entry_id_key OR (source.journal_entry_id_key IS NULL AND target.journal_entry_id_key IS NULL))
            AND (target.posting_date_key = source.posting_date_key OR (source.posting_date_key IS NULL AND target.posting_date_key IS NULL))
    WHERE
        coalesce(finance_dw.fact_general_ledger.debit_amount, 0.0) != coalesce(source.debit_amount, 0.0)
        OR coalesce(finance_dw.fact_general_ledger.credit_amount, 0.0) != coalesce(source.credit_amount, 0.0)
        OR coalesce(finance_dw.fact_general_ledger.amount, 0.0) != coalesce(source.amount, 0.0)
) AS update_old_rows
WHERE finance_dw.fact_general_ledger.fact_general_ledger_key = update_old_rows.fact_general_ledger_key
; 


DELETE FROM finance_dw.fact_general_ledger USING (SELECT DISTINCT target.fact_general_ledger_key AS fact_general_ledger_key 
FROM finance_dw.fact_general_ledger AS target LEFT OUTER JOIN finance_etl.fact_general_ledger_source AS source ON (source.gl_account_id_key = target.gl_account_id_key OR (source.gl_account_id_key IS NULL AND target.gl_account_id_key IS NULL)) AND (source.branch_key = target.branch_key OR (source.branch_key IS NULL AND target.branch_key IS NULL)) AND (source.corporation_key = target.corporation_key OR (source.corporation_key IS NULL AND target.corporation_key IS NULL)) AND (source.category_key = target.category_key OR (source.category_key IS NULL AND target.category_key IS NULL)) AND (source.description_key = target.description_key OR (source.description_key IS NULL AND target.description_key IS NULL)) AND (source.journal_entry_id_key = target.journal_entry_id_key OR (source.journal_entry_id_key IS NULL AND target.journal_entry_id_key IS NULL)) AND (source.posting_date_key = target.posting_date_key OR (source.posting_date_key IS NULL AND target.posting_date_key IS NULL)) 
WHERE source.fact_general_ledger_key IS NULL) AS soft_delete_subquery WHERE finance_dw.fact_general_ledger.fact_general_ledger_key = soft_delete_subquery.fact_general_ledger_key
;
END;
$$
