CREATE OR REPLACE PROCEDURE finance_etl.sp_populate_source_table_fact_general_ledger()
AS $$
BEGIN

INSERT INTO finance_etl.fact_general_ledger_source
(
    gl_account_id_key,
    branch_key,
    corporation_key,
    category_key,
    description_key,
    journal_entry_id_key,
    posting_date_key,
    debit_amount ,
    credit_amount ,
    amount
)

    select
    coalesce(a.gl_account_id_key, -1) as gl_account_id_key,
    coalesce(b.branch_key,  -1) as branch_key,
    coalesce(cp.corporation_key,  -1) as corporation_key,
    coalesce(ct.category_key,  -1) as category_key,
    coalesce(j.description_key, -1) as description_key,
    coalesce(ji.journal_entry_id_key, -1) as journal_entry_id_key,
    coalesce(c.date_key, -1) as posting_date_key,
    sum(f.rbdr) as debit_amount ,
    sum(f.rbcr) as credit_amount,
    sum(f.rbdr - f.rbcr) as amount
    from finance_staging.iseries_usrjmflib_arfadtv110 f
    left join finance_dw.dim_account                a  on a.gl_account_id       = f.rbglcd
    left join finance_dw.dim_corporation            cp on cp.corporation        = f.rbcorp
    left join finance_dw.dim_category               ct on ct.category           = a.gl_category
    left join finance_dw.dim_calendar               c  on c.calendar_date       = last_day(f.glperiod)
    left join finance_dw.dim_journal_entry          ji on ji.journal_entry_id   = f.rbseq
    left join finance_dw.dim_journal_description    j  on j.description         = f.rbdesc
    left join finance_dw.dim_branch                 b  on b.branch              = f.rbbr and c.calendar_date between b.SCD2_start_date and b.SCD2_end_date

    where f.rbdesc != 'Closing Retained'

    group by
    a.gl_account_id_key,
    b.branch_key,
    cp.corporation_key,
    ct.category_key,
    j.description_key,
    ji.journal_entry_id_key,
    c.date_key
;
END;
$$ LANGUAGE plpgsql;
