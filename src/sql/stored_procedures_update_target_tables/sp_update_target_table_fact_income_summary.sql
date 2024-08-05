CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_fact_income_summary()
LANGUAGE plpgsql
AS $$
BEGIN

CALL finance_etl.sp_update_source_table_fact_income_summary();

DELETE FROM finance_dw.fact_income_summary
USING finance_etl.fact_income_summary_source source
WHERE action = 'DELETE' and source.foreign_key_hash = finance_dw.fact_income_summary.foreign_key_hash
;

UPDATE finance_dw.fact_income_summary
SET
    measures_hash        = source.measures_hash       ,
    gl_account_id_key    = source.gl_account_id_key   ,
    branch_key           = source.branch_key          ,
    corporation_key      = source.corporation_key     ,
    posting_date_key     = source.posting_date_key    ,
    debit_amount         = source.debit_amount        ,
    credit_amount        = source.credit_amount       ,
    amount               = source.amount
FROM finance_etl.fact_income_summary_source source
WHERE
    source.action = 'UPDATE' and
    source.foreign_key_hash = finance_dw.fact_income_summary.foreign_key_hash
;

INSERT INTO finance_dw.fact_income_summary (
    foreign_key_hash,
    measures_hash,
    gl_account_id_key,
    branch_key,
    corporation_key,
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
posting_date_key,
debit_amount,
credit_amount,
amount
from finance_etl.fact_income_summary_source
where action = 'INSERT'
;

END;
$$
