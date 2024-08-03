CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_fact_acquisition_cash_flow()
LANGUAGE plpgsql
AS $$
BEGIN

with base as
(
    select
    coalesce(c.date_key                       , -1) as posting_date_key               ,
    coalesce(co.corporation_key               , -1) as corporation_key                ,
    coalesce(i.indirect_cash_flow_category_key, -1) as indirect_cash_flow_category_key
    from finance_staging.flat_file_acquisition_cash_flow f
    left join finance_dw.dim_calendar                     c on c.calendar_date = f.date
    left join finance_dw.dim_indirect_cash_flow_category  i on i.indirect_cash_flow_category = f.indirect_cash_flow_category
    left join finance_dw.dim_corporation                 co on co.corporation = f.corporation
    where i.indirect_cash_flow_category is not null
)

, soft_delete_cte as
(
    SELECT distinct
    target.fact_acquisition_key
    FROM finance_dw.fact_acquisition AS target
    LEFT OUTER JOIN base AS source ON
    source.posting_date_key                = target.posting_date_key                and
    source.corporation_key                 = target.corporation_key                 and
    source.indirect_cash_flow_category_key = target.indirect_cash_flow_category_key
    WHERE
    source.posting_date_key is null
    OR source.corporation_key  null
    OR source.indirect_cash_flow_category_key is null
)

DELETE FROM finance_dw.fact_acquisition_cash_flow
USING soft_delete_cte
WHERE finance_dw.fact_acquisition_cash_flow.fact_acquisition_cash_flow_key = soft_delete_cte.fact_acquisition_cash_flow_key
;

INSERT INTO finance_dw.fact_acquisition_cash_flow (
    posting_date_key,
    corporation_key,
    indirect_cash_flow_category_key,
    cash_flow
)

with base as
(
    select
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
source.posting_date_key               ,
source.corporation_key                ,
source.indirect_cash_flow_category_key,
source.cash_flow
FROM base AS source
LEFT OUTER JOIN finance_dw.fact_acquisition_cash_flow AS target ON
source.posting_date_key                    = target.source.posting_date_key               ,
AND source.corporation_key                 = target.source.corporation_key                ,
AND source.indirect_cash_flow_category_key = target.source.indirect_cash_flow_category_key,
WHERE target.fact_acquisition_cash_flow_key IS NULL
;
END;
$$
