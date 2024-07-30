CREATE OR REPLACE PROCEDURE finance_etl.sp_create_fact_cash_flow()
AS $$
BEGIN

INSERT INTO finance_etl.fact_cash_flow_source
(
    gl_account_id_key,
    branch_key,
    corporation_key,
    category_key,
    indirect_cash_flow_category_key,
    posting_date_key,
    general_ledger ,
    acquisition ,
    cash_flow
)

with base as
(
    select
    cast('general_ledger' as VARCHAR(30)) as type,
    coalesce(f.gl_account_id_key, -1) as gl_account_id_key,
    coalesce(f.branch_key, -1) as branch_key,
    coalesce(f.corporation_key, -1) as corporation_key,
    coalesce(f.category_key, -1) as category_key,
    coalesce(cf.indirect_cash_flow_category_key, -1) as indirect_cash_flow_category_key,
    coalesce(f.posting_date_key, -1) as posting_date_key,
    case when cf.reverse = 1 then f.amount * -1 else f.amount end as amount
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
    f.cash_flow as amount
    from finance_dw.fact_acquisition_cash_flow f
    left join finance_dw.dim_corporation c on c.corporation_key = f.corporation_key
    left join finance_dw.dim_calendar cl on cl.date_key = f.posting_date_key
    left join finance_dw.dim_indirect_cash_flow_category i on i.indirect_cash_flow_category_key = f.indirect_cash_flow_category_key
    where i.indirect_cash_flow_category_key is not null
)

, cash_flow as
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
from cash_flow
PIVOT(SUM(amount) for type in ('general_ledger', 'acquisition'))
;

END;

$$ LANGUAGE plpgsql;
