
CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_fact_acquisition_cash_flow()
LANGUAGE plpgsql
AS $$
BEGIN

CALL finance_etl.sp_populate_source_table_fact_acquisition_cash_flow();

INSERT INTO finance_dw.fact_acquisition_cash_flow (posting_date_key, corporation_key, indirect_cash_flow_category_key, cash_flow)
SELECT
    source.posting_date_key,
    source.corporation_key,
    source.indirect_cash_flow_category_key,
    source.cash_flow
FROM finance_etl.fact_acquisition_cash_flow_source AS source
LEFT OUTER JOIN
    finance_dw.fact_acquisition_cash_flow AS target
    ON
        (source.posting_date_key = target.posting_date_key OR (source.posting_date_key IS NULL AND target.posting_date_key IS NULL))
        AND (source.corporation_key = target.corporation_key OR (source.corporation_key IS NULL AND target.corporation_key IS NULL))
        AND (
            source.indirect_cash_flow_category_key = target.indirect_cash_flow_category_key
            OR (source.indirect_cash_flow_category_key IS NULL AND target.indirect_cash_flow_category_key IS NULL)
        )
WHERE target.fact_acquisition_cash_flow_key IS NULL
; 


UPDATE finance_dw.fact_acquisition_cash_flow SET cash_flow = update_old_rows.cash_flow FROM (
    SELECT
        target.fact_acquisition_cash_flow_key,
        source.posting_date_key,
        source.corporation_key,
        source.indirect_cash_flow_category_key,
        source.cash_flow
    FROM finance_dw.fact_acquisition_cash_flow, finance_dw.fact_acquisition_cash_flow AS target
    INNER JOIN
        finance_etl.fact_acquisition_cash_flow_source AS source
        ON
            (target.posting_date_key = source.posting_date_key OR (source.posting_date_key IS NULL AND target.posting_date_key IS NULL))
            AND (target.corporation_key = source.corporation_key OR (source.corporation_key IS NULL AND target.corporation_key IS NULL))
            AND (
                target.indirect_cash_flow_category_key = source.indirect_cash_flow_category_key
                OR (source.indirect_cash_flow_category_key IS NULL AND target.indirect_cash_flow_category_key IS NULL)
            )
    WHERE coalesce(finance_dw.fact_acquisition_cash_flow.cash_flow, 0.0) != coalesce(source.cash_flow, 0.0)
) AS update_old_rows
WHERE finance_dw.fact_acquisition_cash_flow.fact_acquisition_cash_flow_key = update_old_rows.fact_acquisition_cash_flow_key
; 


DELETE FROM finance_dw.fact_acquisition_cash_flow USING (SELECT DISTINCT target.fact_acquisition_cash_flow_key AS fact_acquisition_cash_flow_key 
FROM finance_dw.fact_acquisition_cash_flow AS target LEFT OUTER JOIN finance_etl.fact_acquisition_cash_flow_source AS source ON (source.posting_date_key = target.posting_date_key OR (source.posting_date_key IS NULL AND target.posting_date_key IS NULL)) AND (source.corporation_key = target.corporation_key OR (source.corporation_key IS NULL AND target.corporation_key IS NULL)) AND (source.indirect_cash_flow_category_key = target.indirect_cash_flow_category_key OR (source.indirect_cash_flow_category_key IS NULL AND target.indirect_cash_flow_category_key IS NULL)) 
WHERE source.fact_acquisition_cash_flow_key IS NULL) AS soft_delete_subquery WHERE finance_dw.fact_acquisition_cash_flow.fact_acquisition_cash_flow_key = soft_delete_subquery.fact_acquisition_cash_flow_key
;
END;
$$
