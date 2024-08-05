CREATE OR REPLACE PROCEDURE finance_etl.sp_update_source_table_fact_income_summary()
LANGUAGE plpgsql
AS $$
BEGIN

truncate table finance_etl.fact_income_summary_source;

INSERT INTO finance_etl.fact_income_summary_source (
    foreign_key_hash,
    measures_hash,
    branch_key,
    gl_account_id_key,
    corporation_key,
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
    CAST(coalesce(cast(date_part(year, f.glperiod) || 1231 as INT), -1) AS VARCHAR)
) AS foreign_key_hash,
FNV_HASH(
    CAST(sum(f.rbdr) AS VARCHAR) ||
    CAST(sum(f.rbcr) AS VARCHAR)
    ) AS measures_hash,
coalesce(re.gl_account_id_key, -1) as gl_account_id_key,
coalesce(b.branch_key        , -1) as branch_key,
coalesce(co.corporation_key  , -1) as corporation_key,
cast(date_part(year, f.glperiod) || 1231 as INT) as posting_date_key,
sum(coalesce(f.rbdr         , 0)) as debit_amount,
sum(coalesce(f.rbcr         , 0)) as credit_amount,
sum(coalesce(f.rbcr - f.rbdr, 0)) as amount
from finance_staging.iseries_usrjmflib_arfadtv110 f
left join finance_dw.dim_account a on a.gl_account_id = f.rbglcd
left join finance_dw.dim_corporation co on co.corporation = f.rbcorp
left join finance_dw.dim_branch b on
    b.branch = co.elimination_branch and
    cast(date_part(year, f.glperiod) || '-12-31' as DATE) between b.SCD2_start_date and b.scd2_end_date
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
)

SELECT
    source.foreign_key_hash,
    source.measures_hash,
    source.gl_account_id_key,
    source.branch_key,
    source.corporation_key,
    source.posting_date_key,
    source.debit_amount,
    source.credit_amount,
    source.amount,
    'INSERT' as action
FROM base AS source
LEFT OUTER JOIN finance_dw.fact_income_summary AS target ON
    source.foreign_key_hash = target.foreign_key_hash
WHERE target.foreign_key_hash is null

union all

SELECT
    target.foreign_key_hash,
    target.measures_hash,
    target.gl_account_id_key,
    target.branch_key,
    target.corporation_key,
    target.posting_date_key,
    target.debit_amount,
    target.credit_amount,
    target.amount,
    'DELETE' as action
FROM base AS source
LEFT OUTER JOIN finance_dw.fact_income_summary AS target ON
    source.foreign_key_hash = target.foreign_key_hash
WHERE source.foreign_key_hash is null

union all

SELECT
    source.foreign_key_hash,
    source.measures_hash,
    source.gl_account_id_key,
    source.branch_key,
    source.corporation_key,
    source.posting_date_key,
    source.debit_amount,
    source.credit_amount,
    source.amount,
    'UPDATE' as action
FROM base AS source
LEFT OUTER JOIN finance_dw.fact_income_summary AS target ON
    source.foreign_key_hash = target.foreign_key_hash
WHERE
    source.foreign_key_hash is not null and
    target.foreign_key_hash is not null and
    source.measures_hash != target.measures_hash
;
END;
$$
