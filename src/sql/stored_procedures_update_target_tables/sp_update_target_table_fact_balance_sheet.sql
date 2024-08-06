CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_fact_balance_sheet()
LANGUAGE plpgsql
AS $$
BEGIN

CALL finance_etl.sp_update_source_table_fact_balance_sheet();

DELETE FROM finance_dw.fact_balance_sheet
USING finance_etl.fact_balance_sheet_source source
WHERE action = 'DELETE' and source.foreign_key_hash = finance_dw.fact_balance_sheet.foreign_key_hash
;

UPDATE finance_dw.fact_balance_sheet
SET
    measures_hash     = source.measures_hash       ,
    gl_account_id_key = source.gl_account_id_key   ,
    branch_key        = source.branch_key          ,
    corporation_key   = source.corporation_key     ,
    category_key      = source.category_key        ,
    posting_date_key  = source.posting_date_key    ,
    debit_balance     = source.debit_balance        ,
    credit_balance    = source.credit_balance       ,
    balance           = source.balance
FROM finance_etl.fact_balance_sheet_source source
WHERE
    source.action = 'UPDATE' and
    source.foreign_key_hash = finance_dw.fact_balance_sheet.foreign_key_hash
;

INSERT INTO finance_dw.fact_balance_sheet (
    foreign_key_hash,
    measures_hash,
    gl_account_id_key,
    branch_key,
    corporation_key,
    category_key,
    posting_date_key,
    debit_balance,
    credit_balance,
    balance
)

select
foreign_key_hash,
measures_hash,
gl_account_id_key,
branch_key,
corporation_key,
category_key,
posting_date_key,
debit_balance,
credit_balance,
balance
from finance_etl.fact_balance_sheet_source
where action = 'INSERT'

;
END;
$$
