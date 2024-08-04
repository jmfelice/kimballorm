
CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_fact_acquisition_cash_flow()
LANGUAGE plpgsql
AS $$
BEGIN

CALL finance_etl.sp_update_source_table_fact_acquisition_cash_flow();

DELETE FROM finance_dw.fact_acquisition_cash_flow
USING finance_etl.fact_acquisition_cash_flow_source source
WHERE action = 'DELETE' and source.foreign_key_hash = finance_dw.fact_acquisition_cash_flow.foreign_key_hash
;

UPDATE finance_dw.fact_acquisition_cash_flow
SET
    measures_hash                   = source.measures_hash,
    posting_date_key                = source.posting_date_key,
    corporation_key                 = source.corporation_key,
    indirect_cash_flow_category_key = source.indirect_cash_flow_category_key,
    cash_flow                       = source.cash_flow
FROM finance_etl.fact_acquisition_cash_flow_source source
WHERE
    source.action = 'UPDATE' and
    source.foreign_key_hash = finance_dw.fact_acquisition_cash_flow.foreign_key_hash
;

INSERT INTO finance_dw.fact_acquisition_cash_flow (
foreign_key_hash,
measures_hash,
posting_date_key,
corporation_key,
indirect_cash_flow_category_key,
cash_flow
)

select
foreign_key_hash,
measures_hash,
posting_date_key,
corporation_key,
indirect_cash_flow_category_key,
cash_flow
from finance_etl.fact_acquisition_cash_flow_source
where action = 'INSERT'
;

END;
$$
