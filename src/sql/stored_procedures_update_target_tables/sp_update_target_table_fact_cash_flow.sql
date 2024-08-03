CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_fact_acquisition_cash_flow()
LANGUAGE plpgsql
AS $$
BEGIN

with base as
(
    select
    cast('general_ledger' as VARCHAR(30)) as type,
    coalesce(f.gl_account_id_key               , -1) as gl_account_id_key,
    coalesce(f.branch_key                      , -1) as branch_key,
    coalesce(f.corporation_key                 , -1) as corporation_key,
    coalesce(f.category_key                    , -1) as category_key,
    coalesce(cf.indirect_cash_flow_category_key, -1) as indirect_cash_flow_category_key,
    coalesce(f.posting_date_key                , -1) as posting_date_key
    from finance_dw.fact_general_ledger f
    left join finance_dw.bridge_map_cash_flow cf on cf.gl_account_id_key = f.gl_account_id_key
    where cf.indirect_cash_flow_category_key is not null

    union all

    select
    cast('acquisition' as VARCHAR(30)) as type,
    -1                                              as gl_account_id_key,
    -1                                              as branch_key,
    coalesce(f.corporation_key                , -1) as corporation_key,
    -1                                              as category_key ,
    coalesce(i.indirect_cash_flow_category_key, -1) as indirect_cash_flow_category_key,
    coalesce(f.posting_date_key               , -1) as posting_date_key
    from finance_dw.fact_acquisition_cash_flow f
    left join finance_dw.dim_corporation                  c on c.corporation_key = f.corporation_key
    left join finance_dw.dim_calendar                    cl on cl.date_key = f.posting_date_key
    left join finance_dw.dim_indirect_cash_flow_category  i on i.indirect_cash_flow_category_key = f.indirect_cash_flow_category_key
    where i.indirect_cash_flow_category_key is not null
)

, delete_cte as
(
    SELECT
    target.fact_cash_flow_key
    FROM finance_dw.fact_cash_flow AS target
    LEFT OUTER JOIN base AS source ON
        source.gl_account_id_key               = target.gl_account_id_key               and
        source.branch_key                      = target.branch_key                      and
        source.corporation_key                 = target.corporation_key                 and
        source.category_key                    = target.category_key                    and
        source.indirect_cash_flow_category_key = target.indirect_cash_flow_category_key and
        source.posting_date_key                = target.posting_date_key
    WHERE
        source.gl_account_id_key               is null or
        source.branch_key                      is null or
        source.corporation_key                 is null or
        source.category_key                    is null or
        source.indirect_cash_flow_category_key is null or
        source.posting_date_key                is null
)

DELETE FROM finance_dw.fact_cash_flow
USING delete_cte
WHERE finance_dw.fact_acquisition_cash_flow.fact_cash_flow_key = delete_cte.fact_cash_flow_key
;

INSERT INTO finance_dw.fact_cash_flow (
    gl_account_id_key,
    branch_key,
    corporation_key,
    category_key ,
    indirect_cash_flow_category_key,
    posting_date_key,
    general_ledger,
    acquisition,
    cash_flow
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
    coalesce(f.posting_date_key                , -1) as posting_date_key
    from finance_dw.fact_general_ledger f
    left join finance_dw.bridge_map_cash_flow cf on cf.gl_account_id_key = f.gl_account_id_key
    where cf.indirect_cash_flow_category_key is not null

    union all

    select
    cast('acquisition' as VARCHAR(30)) as type,
    -1                                              as gl_account_id_key,
    -1                                              as branch_key,
    coalesce(f.corporation_key                , -1) as corporation_key,
    -1                                              as category_key ,
    coalesce(i.indirect_cash_flow_category_key, -1) as indirect_cash_flow_category_key,
    coalesce(f.posting_date_key               , -1) as posting_date_key
    from finance_dw.fact_acquisition_cash_flow f
    left join finance_dw.dim_corporation                  c on c.corporation_key = f.corporation_key
    left join finance_dw.dim_calendar                    cl on cl.date_key = f.posting_date_key
    left join finance_dw.dim_indirect_cash_flow_category  i on i.indirect_cash_flow_category_key = f.indirect_cash_flow_category_key
    where i.indirect_cash_flow_category_key is not null
)

, combined as
(
    select
    type,
    gl_account_id_key,
    branch_key,
    corporation_key,
    category_key ,
    indirect_cash_flow_category_key,
    posting_date_key,
    sum(amount) * -1 as amount
    from base f

    group by
    type,
    gl_account_id_key,
    branch_key,
    corporation_key,
    category_key ,
    indirect_cash_flow_category_key,
    posting_date_key
)

, cash_flow as
(
    select
    gl_account_id_key,
    branch_key,
    corporation_key,
    category_key ,
    indirect_cash_flow_category_key,
    posting_date_key,
    coalesce(general_ledger, 0) as general_ledger,
    coalesce(acquisition, 0) as acquisition,
    coalesce(general_ledger, 0) + coalesce(acquisition, 0) as cash_flow
    from combined
    PIVOT(SUM(amount) for type in ('general_ledger', 'acquisition'))
)

SELECT
    source.gl_account_id_key,
    source.branch_key,
    source.corporation_key,
    source.category_key ,
    source.indirect_cash_flow_category_key,
    source.posting_date_key,
    source.general_ledger,
    source.acquisition,
    source.cash_flow
FROM cash_flow AS source
LEFT OUTER JOIN finance_dw.fact_cash_flow AS target ON
    source.gl_account_id_key               = target.gl_account_id_key              ,
    source.branch_key                      = target.branch_key                     ,
    source.corporation_key                 = target.corporation_key                ,
    source.category_key                    = target.category_key                   ,
    source.indirect_cash_flow_category_key = target.indirect_cash_flow_category_key,
    source.posting_date_key                = target.posting_date_key
WHERE target.fact_cash_flow_key IS NULL
;
END;
$$
