create or replace procedure finance_etl.sp_update_source_table_dim_product_line()
as $$
begin

insert into finance_etl.dim_product_line_source
(
    product_line_key,
    product_line,
    product_line_description,
    unit_of_measure,
    scd2_start_date,
    scd2_end_date,
    current_flag,
    active
)

with period_ending as
(
    select current_month_ending as period_ending
    from finance_staging.last_date_of_mhf_schema
)

, base as
(
    select
    row_number() over(partition by riline order by riline) as line_count,
    riline as product_line,
    ridesc as product_line_description,
    riuom  as unit_of_measure,
    cast((select period_ending from period_ending) as date) as source
    from finance_staging.iseries_rellib_reflines
    where risub = 0
)

select
-1 as product_line_key,
cast(null as int) as product_line,
'None' as product_line_description,
'None' as unit_of_measure,
cast('1900-01-01' as date) as scd2_start_date,
cast('2999-12-31' as date) as scd2_end_date,
0 as current_flag,
0 as active

union all

select
coalesce((select max(product_line_key) from finance_dw.dim_product_line), 0) +
    row_number() over(order by product_line) as product_line_key,
product_line,
product_line_description,
coalesce(unit_of_measure, 'None') as unit_of_measure,
source as scd2_start_date,
cast('2999-12-31' as date) as scd2_end_date,
1 as current_flag,
1 as active
from base
where line_count = 1

;
end;
$$ language plpgsql;
