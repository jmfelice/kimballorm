CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_fact_general_ledger()
LANGUAGE plpgsql
AS $$
BEGIN

with base as
(
    select

    coalesce(a.gl_account_id_key    , -1) as gl_account_id_key,
    coalesce(b.branch_key           , -1) as branch_key,
    coalesce(cp.corporation_key     , -1) as corporation_key,
    coalesce(ct.category_key        , -1) as category_key,
    coalesce(j.description_key      , -1) as description_key,
    coalesce(ji.journal_entry_id_key, -1) as journal_entry_id_key,
    coalesce(c.date_key             , -1) as posting_date_key

    from finance_staging.iseries_usrjmflib_arfadtv110 f
    left join finance_dw.dim_account                a  on a.gl_account_id       = f.rbglcd
    left join finance_dw.dim_corporation            cp on cp.corporation        = f.rbcorp
    left join finance_dw.dim_category               ct on ct.category           = a.gl_category
    left join finance_dw.dim_calendar               c  on c.calendar_date       = last_day(f.glperiod)
    left join finance_dw.dim_journal_entry          ji on ji.journal_entry_id   = f.rbseq
    left join finance_dw.dim_journal_description    j  on j.description         = f.rbdesc
    left join finance_dw.dim_branch                 b  on b.branch              = f.rbbr and c.calendar_date between b.SCD2_start_date and b.SCD2_end_date
    where f.rbdesc != 'Closing Retained'
)

, source_time_periods as
(
    select
    max(posting_date_key) as max_date,
    min(posting_date_key) as min_date
    from base
)

, soft_delete_cte as
(

SELECT distinct
target.fact_general_ledger_key
FROM finance_dw.fact_general_ledger AS target
LEFT OUTER JOIN base AS source ON
    source.gl_account_id_key        = target.gl_account_id_key
    AND source.branch_key           = target.branch_key
    AND source.corporation_key      = target.corporation_key
    AND source.category_key         = target.category_key
    AND source.description_key      = target.description_key
    AND source.journal_entry_id_key = target.journal_entry_id_key
    AND source.posting_date_key     = target.posting_date_key

WHERE
    target.posting_date_key between
        (select min_date from source_time_periods) and
        (select max_date from source_time_periods) and
    (
    source.gl_account_id_key       is null
    OR source.branch_key           is null
    OR source.corporation_key      is null
    OR source.category_key         is null
    OR source.description_key      is null
    OR source.journal_entry_id_key is null
    OR source.posting_date_key     is null
    )
)

DELETE FROM finance_dw.fact_general_ledger
USING soft_delete_cte
WHERE finance_dw.fact_general_ledger.fact_general_ledger_key = soft_delete_cte.fact_general_ledger_key
;

INSERT INTO finance_dw.fact_general_ledger (
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

with base as
(
    select

    coalesce(a.gl_account_id_key    , -1) as gl_account_id_key,
    coalesce(b.branch_key           , -1) as branch_key,
    coalesce(cp.corporation_key     , -1) as corporation_key,
    coalesce(ct.category_key        , -1) as category_key,
    coalesce(j.description_key      , -1) as description_key,
    coalesce(ji.journal_entry_id_key, -1) as journal_entry_id_key,
    coalesce(c.date_key             , -1) as posting_date_key,
    sum(f.rbdr)          as debit_amount ,
    sum(f.rbcr)          as credit_amount,
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
)

, source_time_periods as
(
    select
    max(posting_date_key) as max_date,
    min(posting_date_key) as min_date
    from base
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
FROM base AS source
LEFT OUTER JOIN finance_dw.fact_general_ledger AS target ON
    source.gl_account_id_key        = target.gl_account_id_key
    AND source.branch_key           = target.branch_key
    AND source.corporation_key      = target.corporation_key
    AND source.category_key         = target.category_key
    AND source.description_key      = target.description_key
    AND source.journal_entry_id_key = target.journal_entry_id_key
    AND source.posting_date_key     = target.posting_date_key
WHERE target.fact_general_ledger_key IS NULL

;
END;
$$
