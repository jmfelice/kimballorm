CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_fact_general_ledger()
LANGUAGE plpgsql
AS $$
BEGIN

CALL finance_etl.sp_update_source_table_fact_general_ledger();

DELETE FROM finance_dw.fact_general_ledger
USING finance_etl.fact_general_ledger_source source
WHERE action = 'DELETE' and source.foreign_key_hash = finance_dw.fact_general_ledger.foreign_key_hash
;

UPDATE finance_dw.fact_general_ledger
SET
    measures_hash        = source.measures_hash       ,
    gl_account_id_key    = source.gl_account_id_key   ,
    branch_key           = source.branch_key          ,
    corporation_key      = source.corporation_key     ,
    category_key         = source.category_key        ,
    description_key      = source.description_key     ,
    journal_entry_id_key = source.journal_entry_id_key,
    posting_date_key     = source.posting_date_key    ,
    debit_amount         = source.debit_amount        ,
    credit_amount        = source.credit_amount       ,
    amount               = source.amount
FROM finance_etl.fact_general_ledger_source source
WHERE
    source.action = 'UPDATE' and
    source.foreign_key_hash = finance_dw.fact_general_ledger.foreign_key_hash
;

INSERT INTO finance_dw.fact_general_ledger (
    foreign_key_hash,
    measures_hash,
    gl_account_id_key,
    branch_key,
    corporation_key,
    category_key,
    description_key,
    journal_entry_id_key,
    posting_date_key,
    debit_amount,
    credit_amount,
    amount
)

select
foreign_key_hash,
measures_hash,
gl_account_id_key,
branch_key,
corporation_key,
category_key,
description_key,
journal_entry_id_key,
posting_date_key,
debit_amount,
credit_amount,
amount
from finance_etl.fact_general_ledger_source
where action = 'INSERT'
;

END;
$$
