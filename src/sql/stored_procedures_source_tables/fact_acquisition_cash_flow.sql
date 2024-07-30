CREATE OR REPLACE PROCEDURE finance_etl.sp_populate_source_table_fact_acquisition_cash_flow()
AS $$
BEGIN

INSERT INTO finance_etl.fact_acquisition_cash_flow_source
(
    posting_date_key,
    corporation_key,
    indirect_cash_flow_category_key,
    cash_flow
)

select
c.date_key as posting_date_key,
co.corporation_key,
i.indirect_cash_flow_category_key,
cast(sum(f.amount) as decimal(20, 8)) as cash_flow
from finance_staging.flat_file_acquisition_cash_flow f
left join finance_dw.dim_calendar c on c.calendar_date = f.date
left join finance_dw.dim_indirect_cash_flow_category i on i.indirect_cash_flow_category = f.indirect_cash_flow_category
left join finance_dw.dim_corporation co on co.corporation = f.corporation

where i.indirect_cash_flow_category is not null

group by
c.date_key,
co.corporation_key,
i.indirect_cash_flow_category_key

;

END;
$$ LANGUAGE plpgsql;
