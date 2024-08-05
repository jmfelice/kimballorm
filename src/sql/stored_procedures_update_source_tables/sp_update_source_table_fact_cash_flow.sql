CREATE OR REPLACE PROCEDURE finance_etl.sp_update_source_table_fact_cash_flow()
LANGUAGE plpgsql
AS $$
BEGIN

truncate table finance_etl.fact_cash_flow_source;

insert into finance_etl.fact_cash_flow_source
(
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
    action
)

with base as
(
        select
        cast('general_ledger' as VARCHAR(30)) as type,
        coalesce(f.gl_account_id_key               , -1) as gl_account_id_key,
        coalesce(f.branch_key                      , -1) as branch_key,
        coalesce(f.corporation_key                 , -1) as corporation_key,
        coalesce(f.category_key                    , -1) as category_key,
        coalesce(cf.indirect_cash_flow_category_key, -1) as indirect_cash_flow_category_key,
        coalesce(f.posting_date_key                , -1) as posting_date_key,
        case when cf.reverse = 1 then coalesce(f.amount, 0) * -1 else coalesce(f.amount, 0) end as amount
        from finance_dw.fact_general_ledger f
        left join finance_dw.bridge_map_cash_flow cf on cf.gl_account_id_key = f.gl_account_id_key
        where cf.indirect_cash_flow_category_key is not null

        union all

        select
        cast('acquisition' as VARCHAR(30)) as type,
        -1 as gl_account_id_key,
        -1 as branch_key,
        coalesce(f.corporation_key, -1) as corporation_key,
        -1 as category_key ,
        coalesce(i.indirect_cash_flow_category_key, -1) as indirect_cash_flow_category_key,
        coalesce(f.posting_date_key, -1) as posting_date_key,
        coalesce(f.cash_flow, 0) as amount
        from finance_dw.fact_acquisition_cash_flow f
        left join finance_dw.dim_corporation c on c.corporation_key = f.corporation_key
        left join finance_dw.dim_calendar cl on cl.date_key = f.posting_date_key
        left join finance_dw.dim_indirect_cash_flow_category i on i.indirect_cash_flow_category_key = f.indirect_cash_flow_category_key
        where i.indirect_cash_flow_category_key is not null
)

, cash_flow as
(
    select
    FNV_HASH(
        CAST(coalesce(gl_account_id_key              , -1) AS VARCHAR) ||
        CAST(coalesce(branch_key                     , -1) AS VARCHAR) ||
        CAST(coalesce(corporation_key                , -1) AS VARCHAR) ||
        CAST(coalesce(category_key                   , -1) AS VARCHAR) ||
        CAST(coalesce(indirect_cash_flow_category_key, -1) AS VARCHAR) ||
        CAST(coalesce(posting_date_key               , -1) AS VARCHAR)
    ) AS foreign_key_hash,
    FNV_HASH(
        coalesce(general_ledger, 0) AS VARCHAR) ||
        coalesce(acquisition   , 0)
    ) AS measures_hash,
    gl_account_id_key              ,
    branch_key                     ,
    corporation_key                ,
    category_key                   ,
    indirect_cash_flow_category_key,
    posting_date_key               ,
    coalesce(general_ledger, 0) as general_ledger,
    coalesce(acquisition   , 0) as acquisition,
    coalesce(general_ledger, 0) + coalesce(acquisition, 0) as cash_flow
    from combined
    PIVOT(SUM(amount) for type in ('general_ledger', 'acquisition'))
)

SELECT
    source.foreign_key_hash               ,
    source.measures_hash                  ,
    source.gl_account_id_key              ,
    source.branch_key                     ,
    source.corporation_key                ,
    source.category_key                   ,
    source.indirect_cash_flow_category_key,
    source.posting_date_key               ,
    source.general_ledger                 ,
    source.acquisition                    ,
    source.cash_flow                      ,
    'INSERT' as action
FROM cash_flow AS source
LEFT OUTER JOIN finance_dw.fact_cash_flow AS target ON
    source.foreign_key_hash = target.foreign_key_hash
WHERE target.foreign_key_hash is null

union all

SELECT
    target.foreign_key_hash               ,
    target.measures_hash                  ,
    target.gl_account_id_key              ,
    target.branch_key                     ,
    target.corporation_key                ,
    target.category_key                   ,
    target.indirect_cash_flow_category_key,
    target.posting_date_key               ,
    target.general_ledger                 ,
    target.acquisition                    ,
    target.cash_flow                      ,
    'DELETE' as action
FROM cash_flow AS source
LEFT OUTER JOIN finance_dw.fact_cash_flow AS target ON
    source.foreign_key_hash = target.foreign_key_hash
WHERE source.foreign_key_hash is null

union all

SELECT
    source.foreign_key_hash               ,
    source.measures_hash                  ,
    source.gl_account_id_key              ,
    source.branch_key                     ,
    source.corporation_key                ,
    source.category_key                   ,
    source.indirect_cash_flow_category_key,
    source.posting_date_key               ,
    source.general_ledger                 ,
    source.acquisition                    ,
    source.cash_flow                      ,
    'UPDATE' as action
FROM cash_flow AS source
LEFT OUTER JOIN finance_dw.fact_cash_flow AS target ON
    source.foreign_key_hash = target.foreign_key_hash
WHERE
    source.foreign_key_hash is not null and
    target.foreign_key_hash is not null and
    source.measures_hash != target.measures_hash
;
END;
$$
