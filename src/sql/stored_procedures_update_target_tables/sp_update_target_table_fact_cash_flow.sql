
CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_fact_cash_flow()
LANGUAGE plpgsql
AS $$
BEGIN

CALL finance_etl.sp_populate_source_table_fact_cash_flow();

INSERT INTO finance_dw.fact_cash_flow (
    gl_account_id_key, branch_key, corporation_key, category_key, indirect_cash_flow_category_key, posting_date_key, general_ledger, acquisition, cash_flow
)
SELECT
    source.gl_account_id_key,
    source.branch_key,
    source.corporation_key,
    source.category_key,
    source.indirect_cash_flow_category_key,
    source.posting_date_key,
    source.general_ledger,
    source.acquisition,
    source.cash_flow
FROM finance_etl.fact_cash_flow_source AS source
LEFT OUTER JOIN
    finance_dw.fact_cash_flow AS target
    ON
        (source.gl_account_id_key = target.gl_account_id_key OR (source.gl_account_id_key IS NULL AND target.gl_account_id_key IS NULL))
        AND (source.branch_key = target.branch_key OR (source.branch_key IS NULL AND target.branch_key IS NULL))
        AND (source.corporation_key = target.corporation_key OR (source.corporation_key IS NULL AND target.corporation_key IS NULL))
        AND (source.category_key = target.category_key OR (source.category_key IS NULL AND target.category_key IS NULL))
        AND (
            source.indirect_cash_flow_category_key = target.indirect_cash_flow_category_key
            OR (source.indirect_cash_flow_category_key IS NULL AND target.indirect_cash_flow_category_key IS NULL)
        )
        AND (source.posting_date_key = target.posting_date_key OR (source.posting_date_key IS NULL AND target.posting_date_key IS NULL))
WHERE target.fact_cash_flow_key IS NULL
; 


UPDATE finance_dw.fact_cash_flow SET general_ledger = update_old_rows.general_ledger, acquisition = update_old_rows.acquisition, cash_flow = update_old_rows.cash_flow FROM (
    SELECT
        target.fact_cash_flow_key,
        source.gl_account_id_key,
        source.branch_key,
        source.corporation_key,
        source.category_key,
        source.indirect_cash_flow_category_key,
        source.posting_date_key,
        source.general_ledger,
        source.acquisition,
        source.cash_flow
    FROM finance_dw.fact_cash_flow, finance_dw.fact_cash_flow AS target
    INNER JOIN
        finance_etl.fact_cash_flow_source AS source
        ON
            (target.gl_account_id_key = source.gl_account_id_key OR (source.gl_account_id_key IS NULL AND target.gl_account_id_key IS NULL))
            AND (target.branch_key = source.branch_key OR (source.branch_key IS NULL AND target.branch_key IS NULL))
            AND (target.corporation_key = source.corporation_key OR (source.corporation_key IS NULL AND target.corporation_key IS NULL))
            AND (target.category_key = source.category_key OR (source.category_key IS NULL AND target.category_key IS NULL))
            AND (
                target.indirect_cash_flow_category_key = source.indirect_cash_flow_category_key
                OR (source.indirect_cash_flow_category_key IS NULL AND target.indirect_cash_flow_category_key IS NULL)
            )
            AND (target.posting_date_key = source.posting_date_key OR (source.posting_date_key IS NULL AND target.posting_date_key IS NULL))
    WHERE
        coalesce(finance_dw.fact_cash_flow.general_ledger, 0.0) != coalesce(source.general_ledger, 0.0)
        OR coalesce(finance_dw.fact_cash_flow.acquisition, 0.0) != coalesce(source.acquisition, 0.0)
        OR coalesce(finance_dw.fact_cash_flow.cash_flow, 0.0) != coalesce(source.cash_flow, 0.0)
) AS update_old_rows
WHERE finance_dw.fact_cash_flow.fact_cash_flow_key = update_old_rows.fact_cash_flow_key
; 


DELETE FROM finance_dw.fact_cash_flow USING (SELECT DISTINCT target.fact_cash_flow_key AS fact_cash_flow_key 
FROM finance_dw.fact_cash_flow AS target LEFT OUTER JOIN finance_etl.fact_cash_flow_source AS source ON (source.gl_account_id_key = target.gl_account_id_key OR (source.gl_account_id_key IS NULL AND target.gl_account_id_key IS NULL)) AND (source.branch_key = target.branch_key OR (source.branch_key IS NULL AND target.branch_key IS NULL)) AND (source.corporation_key = target.corporation_key OR (source.corporation_key IS NULL AND target.corporation_key IS NULL)) AND (source.category_key = target.category_key OR (source.category_key IS NULL AND target.category_key IS NULL)) AND (source.indirect_cash_flow_category_key = target.indirect_cash_flow_category_key OR (source.indirect_cash_flow_category_key IS NULL AND target.indirect_cash_flow_category_key IS NULL)) AND (source.posting_date_key = target.posting_date_key OR (source.posting_date_key IS NULL AND target.posting_date_key IS NULL)) 
WHERE source.fact_cash_flow_key IS NULL) AS soft_delete_subquery WHERE finance_dw.fact_cash_flow.fact_cash_flow_key = soft_delete_subquery.fact_cash_flow_key
;
END;
$$
