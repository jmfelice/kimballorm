CREATE OR REPLACE PROCEDURE finance_etl.sp_populate_source_table_bridge_category()
LANGUAGE plpgsql
AS $$
BEGIN

INSERT INTO finance_etl.bridge_category_source
    (
    category_key,
    child_category_key,
    category,
    child_category,
    category_order,
    category_class,
    level,
    isleaf
    )

WITH RECURSIVE base as
(
    select
    c.category_key,
    c.category,
    o.category_key as parent_category_key,
    o.category as parent_category,
    c.isleaf
    from finance_dw.dim_category c
    left join finance_dw.dim_category o on c.parent_category = o.category
    where c.category is not null
)

,  hierarchy (category_key, category, parent_category_key, parent_category) as
(
    select
    category_key,
    category,
    parent_category_key,
    parent_category
    from base

    union all

    select
    c.category_key,
    c.category,
    o.parent_category_key,
    o.parent_category
    from hierarchy c
    inner join base o on o.category_key = c.parent_category_key
    where o.parent_category_key is not null
)

, bridge_category as
(
    select
    h.category_key,
    h.category,
    h.parent_category_key,
    h.parent_category
    from hierarchy h
    left join finance_dw.dim_category c on c.category = h.category

    union all

    select
    category_key,
    category,
    category_key as parent_category_key,
    category as parent_category
    from finance_dw.dim_category
)

select
    b.parent_category_key as category_key,
    b.category_key as child_category_key,
    b.parent_category as category,
    b.category as child_category,
    c.category_order,
    c.category_class,
    c.level,
    c.isleaf
from bridge_category b
left join finance_dw.dim_category c on c.category_key = b.parent_category_key
order by category_key, child_category_key

;
END;
$$
