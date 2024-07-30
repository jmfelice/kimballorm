CREATE OR REPLACE PROCEDURE finance_etl.sp_populate_source_table_bridge_indirect_cash_flow_category()
AS $$
BEGIN

INSERT INTO finance_etl.bridge_indirect_cash_flow_category_source
(
indirect_cash_flow_category_key,
child_indirect_cash_flow_category_key,
indirect_cash_flow_category,
child_indirect_cash_flow_category,
indirect_cash_flow_category_order,
level,
isleaf
)

WITH RECURSIVE base as
(
    select
    c.indirect_cash_flow_category_key,
    c.indirect_cash_flow_category,
    o.indirect_cash_flow_category_key as parent_category_key,
    o.indirect_cash_flow_category as parent_indirect_cash_flow_category,
    c.isleaf
    from finance_dw.dim_indirect_cash_flow_category c
    left join finance_dw.dim_indirect_cash_flow_category o on
        c.parent_indirect_cash_flow_category = o.indirect_cash_flow_category
    where c.indirect_cash_flow_category is not null
)

,  hierarchy  (
    indirect_cash_flow_category_key,
    indirect_cash_flow_category,
    parent_category_key,
    parent_indirect_cash_flow_category
    ) as
(
    select
    indirect_cash_flow_category_key,
    indirect_cash_flow_category,
    parent_category_key,
    parent_indirect_cash_flow_category
    from base

    union all

    select
    c.indirect_cash_flow_category_key,
    c.indirect_cash_flow_category,
    o.parent_category_key,
    o.parent_indirect_cash_flow_category
    from hierarchy c
    inner join base o on o.indirect_cash_flow_category_key = c.parent_category_key
    where o.parent_category_key is not null
)

, bridge_category as
(
    select
    h.indirect_cash_flow_category_key,
    h.indirect_cash_flow_category,
    h.parent_category_key,
    h.parent_indirect_cash_flow_category
    from hierarchy h
    left join finance_dw.dim_indirect_cash_flow_category c on
        c.indirect_cash_flow_category = h.indirect_cash_flow_category

    union all

    select
    indirect_cash_flow_category_key,
    indirect_cash_flow_category,
    indirect_cash_flow_category_key as parent_category_key,
    indirect_cash_flow_category as parent_indirect_cash_flow_category
    from finance_dw.dim_indirect_cash_flow_category
)

select distinct
    b.parent_category_key as indirect_cash_flow_category_key,
    b.indirect_cash_flow_category_key as child_category_key,
    b.parent_indirect_cash_flow_category as indirect_cash_flow_category,
    b.indirect_cash_flow_category as child_category,
    c.indirect_cash_flow_category_order,
    c.level,
    c.isleaf
from bridge_category b
left join finance_dw.dim_indirect_cash_flow_category c on c.indirect_cash_flow_category_key = b.parent_category_key
order by indirect_cash_flow_category_key, child_category_key


;
END;
$$ LANGUAGE plpgsql;
