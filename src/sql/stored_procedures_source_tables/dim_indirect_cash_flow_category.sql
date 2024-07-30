CREATE OR REPLACE PROCEDURE finance_etl.sp_create_dim_indirect_cash_flow_category()
AS $$
BEGIN

INSERT INTO finance_etl.dim_indirect_cash_flow_category_source
(
    indirect_cash_flow_category_key,
    indirect_cash_flow_category,
    parent_indirect_cash_flow_category,
    indirect_cash_flow_category_order,
    isleaf,
    level,
    active
)

with RECURSIVE category_levels (indirect_cash_flow_category, parent_indirect_cash_flow_category, level) as
(
    select
    indirect_cash_flow_category,
    parent_indirect_cash_flow_category,
    0 as level
    from finance_staging.flat_file_indirect_cash_flow_category
    where indirect_cash_flow_category = 'Net Increase / (Decrease) in Cash'

    union all

    select
    o.indirect_cash_flow_category,
    c.indirect_cash_flow_category as parent_indirect_cash_flow_category,
    c.level + 1 as level
    from category_levels c
    inner join finance_staging.flat_file_indirect_cash_flow_category o on
        o.parent_indirect_cash_flow_category = c.indirect_cash_flow_category
)

select
-1 as category_key,
cast(null as varchar(50)) as indirect_cash_flow_category,
cast(null as varchar(50)) as parent_indirect_cash_flow_category,
0 as indirect_cash_flow_category_order,
0 as isleaf,
-1 as level,
0 as active

union

select
coalesce((select max(indirect_cash_flow_category_key) from finance_dw.dim_indirect_cash_flow_category), 0) +
    row_number() over(order by indirect_cash_flow_category_order) as indirect_cash_flow_category_key,
coalesce(c.indirect_cash_flow_category, '') as indirect_cash_flow_category,
coalesce(c.parent_indirect_cash_flow_category, '') as parent_indirect_cash_flow_category,
c.indirect_cash_flow_category_order,
c.isleaf,
l.level,
1 as active
from finance_staging.flat_file_indirect_cash_flow_category c
left join category_levels l on l.indirect_cash_flow_category = c.indirect_cash_flow_category
order by indirect_cash_flow_category_order

;
END;

$$ LANGUAGE plpgsql;
