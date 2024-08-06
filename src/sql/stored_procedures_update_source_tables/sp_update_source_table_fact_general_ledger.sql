CREATE OR REPLACE PROCEDURE finance_etl.sp_update_source_table_fact_general_ledger()
LANGUAGE plpgsql
AS $$
BEGIN

truncate table finance_etl.fact_general_ledger_source;

INSERT INTO finance_etl.fact_general_ledger_source (
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
    amount,
    action
)

with base as
(
    select
    FNV_HASH(
        CAST(coalesce(a.gl_account_id_key    , -1) AS VARCHAR) ||
        CAST(coalesce(b.branch_key           , -1) AS VARCHAR) ||
        CAST(coalesce(cp.corporation_key     , -1) AS VARCHAR) ||
        CAST(coalesce(ct.category_key        , -1) AS VARCHAR) ||
        CAST(coalesce(j.description_key      , -1) AS VARCHAR) ||
        CAST(coalesce(ji.journal_entry_id_key, -1) AS VARCHAR) ||
        CAST(coalesce(c.date_key             , -1) AS VARCHAR)
    ) AS foreign_key_hash,
    FNV_HASH(
        CAST(sum(f.rbdr) AS VARCHAR) ||
        CAST(sum(f.rbcr) AS VARCHAR)
        ) AS measures_hash,
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
    source.foreign_key_hash,
    source.measures_hash,
    source.gl_account_id_key,
    source.branch_key,
    source.corporation_key,
    source.category_key,
    source.description_key,
    source.journal_entry_id_key,
    source.posting_date_key,
    source.debit_amount,
    source.credit_amount,
    source.amount,
    'INSERT' as action
FROM base AS source
LEFT OUTER JOIN finance_dw.fact_general_ledger AS target ON
    source.foreign_key_hash = target.foreign_key_hash
WHERE target.foreign_key_hash is null

union all

SELECT
    target.foreign_key_hash,
    target.measures_hash,
    target.gl_account_id_key,
    target.branch_key,
    target.corporation_key,
    target.category_key,
    target.description_key,
    target.journal_entry_id_key,
    target.posting_date_key,
    target.debit_amount,
    target.credit_amount,
    target.amount,
    'DELETE' as action
FROM finance_dw.fact_general_ledger AS target
LEFT OUTER JOIN base AS source ON
    source.foreign_key_hash = target.foreign_key_hash
WHERE
    target.posting_date_key between
        (select min_date from source_time_periods) and
        (select max_date from source_time_periods) and
    source.foreign_key_hash is null

union all

SELECT
    source.foreign_key_hash,
    source.measures_hash,
    source.gl_account_id_key,
    source.branch_key,
    source.corporation_key,
    source.category_key,
    source.description_key,
    source.journal_entry_id_key,
    source.posting_date_key,
    source.debit_amount,
    source.credit_amount,
    source.amount,
    'UPDATE' as action
FROM base AS source
LEFT OUTER JOIN finance_dw.fact_general_ledger AS target ON
    source.foreign_key_hash = target.foreign_key_hash
WHERE
    source.foreign_key_hash is not null and
    target.foreign_key_hash is not null and
    source.measures_hash != target.measures_hash
;
END;
$$
