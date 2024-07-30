CREATE OR REPLACE PROCEDURE finance_etl.sp_populate_source_table_fact_income_summary()
AS $$
BEGIN

INSERT INTO finance_etl.fact_income_summary_source
(
    branch_key,
    gl_account_id_key,
    corporation_key,
    posting_date_key,
    debit_amount,
    credit_amount,
    amount
)

select
b.branch_key,
re.gl_account_id_key,
co.corporation_key,
cast(date_part(year, f.glperiod) || 1231 as INT) as posting_date_key,
sum(f.rbdr) as debit_amount,
sum(f.rbcr) as credit_amount,
sum(f.rbcr - f.rbdr) as amount
from finance_staging.iseries_usrjmflib_arfadtv110 f
left join finance_dw.dim_account a on a.gl_account_id = f.rbglcd
left join finance_dw.dim_corporation co on co.corporation = f.rbcorp
left join finance_dw.dim_branch b on b.branch = co.elimination_branch and cast(date_part(year, f.glperiod) || '-12-31' as DATE) between b.SCD2_start_date and b.scd2_end_date
left join finance_dw.dim_account re on re.gl_account_id = '31'
where
    f.rbcorp = 1
    and a.account_class in ('R', 'E')
    and f.rbdesc != 'Closing Retained'
group by
    b.branch_key,
    re.gl_account_id_key,
    co.corporation_key,
    cast(date_part(year, f.glperiod) || 1231 as INT)

;

END;
$$ LANGUAGE plpgsql;
