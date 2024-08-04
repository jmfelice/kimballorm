CREATE OR REPLACE PROCEDURE finance_etl.sp_update_source_table_fact_acquisition_cash_flow()
LANGUAGE plpgsql
AS $$
BEGIN

truncate table finance_etl.fact_acquisition_cash_flow_source;

INSERT INTO finance_etl.fact_acquisition_cash_flow_source (
    foreign_key_hash,
    measures_hash,
    posting_date_key,
    corporation_key,
    indirect_cash_flow_category_key,
    cash_flow,
    action
)

with base as
(
    select
    FNV_HASH(
        CAST(coalesce(c.date_key                       , -1) AS VARCHAR) ||
        CAST(coalesce(co.corporation_key               , -1) AS VARCHAR) ||
        CAST(coalesce(i.indirect_cash_flow_category_key, -1) AS VARCHAR)
    ) AS foreign_key_hash,
    FNV_HASH(
        CAST(sum(f.amount) AS VARCHAR)
        ) AS measures_hash,
    coalesce(c.date_key                       , -1) as posting_date_key               ,
    coalesce(co.corporation_key               , -1) as corporation_key                ,
    coalesce(i.indirect_cash_flow_category_key, -1) as indirect_cash_flow_category_key,
    cast(sum(f.amount)           as decimal(20, 8)) as cash_flow
    from finance_staging.flat_file_acquisition_cash_flow f
    left join finance_dw.dim_calendar                     c on c.calendar_date = f.date
    left join finance_dw.dim_indirect_cash_flow_category  i on i.indirect_cash_flow_category = f.indirect_cash_flow_category
    left join finance_dw.dim_corporation                 co on co.corporation = f.corporation

    where i.indirect_cash_flow_category is not null

    group by
    c.date_key,
    co.corporation_key,
    i.indirect_cash_flow_category_key
)

SELECT
    source.foreign_key_hash,
    source.measures_hash,
    source.posting_date_key,
    source.corporation_key,
    source.indirect_cash_flow_category_key,
    source.cash_flow,
    'INSERT' as action
FROM base AS source
LEFT OUTER JOIN finance_dw.fact_acquisition_cash_flow AS target ON
    source.foreign_key_hash = target.foreign_key_hash
WHERE target.foreign_key_hash is null

union all

SELECT
    target.foreign_key_hash,
    target.measures_hash,
    target.posting_date_key,
    target.corporation_key,
    target.indirect_cash_flow_category_key,
    target.cash_flow,
    'DELETE' as action
FROM base AS source
LEFT OUTER JOIN finance_dw.fact_acquisition_cash_flow AS target ON
    source.foreign_key_hash = target.foreign_key_hash
WHERE source.foreign_key_hash is null

union all

SELECT
    source.foreign_key_hash,
    source.measures_hash,
    source.posting_date_key,
    source.corporation_key,
    source.indirect_cash_flow_category_key,
    source.cash_flow,
    'UPDATE' as action
FROM base AS source
LEFT OUTER JOIN finance_dw.fact_acquisition_cash_flow AS target ON
    source.foreign_key_hash = target.foreign_key_hash
WHERE
    source.foreign_key_hash is not null and
    target.foreign_key_hash is not null and
    source.measures_hash != target.measures_hash
;
END;
$$
