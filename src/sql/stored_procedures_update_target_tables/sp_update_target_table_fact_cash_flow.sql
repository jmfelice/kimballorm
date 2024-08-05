CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_fact_cash_flow()
LANGUAGE plpgsql
AS $$
BEGIN

CALL finance_etl.sp_update_source_table_fact_cash_flow();

DELETE FROM finance_dw.fact_cash_flow
USING finance_etl.fact_cash_flow_source source
WHERE action = 'DELETE' and source.foreign_key_hash = finance_dw.fact_cash_flow.foreign_key_hash
;

UPDATE finance_dw.fact_cash_flow
SET
    measures_hash                   = source.measures_hash,
    gl_account_id_key               = source.gl_account_id_key,
    branch_key                      = source.branch_key,
    corporation_key                 = source.corporation_key,
    category_key                    = source.category_key,
    indirect_cash_flow_category_key = source.indirect_cash_flow_category_key,
    posting_date_key                = posting_date_key.measures_hash,
    general_ledger                  = source.general_ledger,
    acquisition                     = source.acquisition,
    cash_flow                       = source.cash_flow
FROM finance_etl.fact_cash_flow_source source
WHERE
    source.action = 'UPDATE' and
    source.foreign_key_hash = finance_dw.fact_cash_flow.foreign_key_hash
;

INSERT INTO finance_dw.fact_cash_flow (
    foreign_key_hash,
    measures_hash,
    gl_account_id_key,
    branch_key,
    corporation_key,
    category_key ,
    indirect_cash_flow_category_key,
    posting_date_key,
    general_ledger,
    acquisition,
    cash_flow
)

select
foreign_key_hash,
measures_hash,
gl_account_id_key,
branch_key,
corporation_key,
category_key ,
indirect_cash_flow_category_key,
posting_date_key,
general_ledger,
acquisition,
cash_flow
from finance_etl.fact_cash_flow_source
where action = 'INSERT'

;
END;
$$
