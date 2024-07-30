CREATE OR REPLACE PROCEDURE finance_etl.sp_create_dim_category()
AS $$
BEGIN

    INSERT INTO finance_etl.dim_category_source
    (
        category_key,
        category,
        parent_category,
        category_class,
        category_order,
        isleaf,
        level,
        active
    )

    with RECURSIVE category_levels (category, parent_category, level) as
    (
        select
        category,
        parent_category,
        0 as level
        from finance_staging.flat_file_category
        where category = 'Master'

        union all

        select
        o.category,
        c.category as parent_category,
        c.level + 1 as level
        from category_levels c
        inner join finance_staging.flat_file_category o on o.parent_category = c.category
    )

    select
    -1 as category_key,
    cast(null as varchar(50)) as category,
    cast(null as varchar(50)) as parent_category,
    cast(null as varchar(1)) as category_class,
    0 as category_order,
    0 as isleaf,
    -1 as level,
    0 as active

    union

    select
    coalesce((select max(category_key) from finance_dw.dim_category), 0) +
        row_number() over(order by c.category_order) as category_key,
    coalesce(c.category, '') as category,
    coalesce(c.parent_category, '') as parent_category,
    coalesce(c.category_class, '') as category_class,
    c.category_order,
    c.isleaf,
    l.level,
    1 as active
    from finance_staging.flat_file_category c
    left join category_levels l on l.category = c.category
    order by category_order
;
END;

$$ LANGUAGE plpgsql;
