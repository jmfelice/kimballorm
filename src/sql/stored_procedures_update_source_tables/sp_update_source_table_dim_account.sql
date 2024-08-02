create or replace procedure finance_etl.sp_populate_source_table_dim_account()
language plpgsql
as $$
begin

    insert into finance_etl.dim_account_source
    (
        gl_account_id_key,
        gl_account_id,
        gl_account_description,
        account_class,
        gl_category,
        intercompany_flag,
        active
    )

    with cleaned_chart_of_accounts as
    (
        select distinct
        gl_account_id as gl_account_id,
        gl_account_description as gl_account_description,
        account_class as account_class,
        gl_category as category,
        cast(intercompany_flag as int) as intercompany_flag
        from finance_staging.flat_file_gl_account
    )

    , original_chart_of_accounts as
    (
        select
        rgacct as gl_account_id,
        rgname as gl_account_description,
        rgtype as account_class,
        cast(null as varchar(50)) as category,
        case when lower(rgname) like '%inter' or lower(rgname) like '%intra%' then 1 else 0 end as intercompany_flag
        from finance_staging.iseries_rellib_refgl
    )

    select
    -1 as gl_account_id_key,
    cast(null as varchar(10)) as gl_account_id,
    'none' as gl_account_description,
    cast(null as varchar(1)) as account_class,
    'none' as category,
    0 as intercompany_flag,
    0 as active

    union all

    select
    coalesce((select max(gl_account_id_key) from finance_dw.dim_account), 0) +
        row_number() over(order by coalesce(c.gl_account_id, o.gl_account_id)) as gl_account_id_key,
    coalesce(c.gl_account_id, o.gl_account_id) as gl_account_id,
    coalesce(c.gl_account_description, o.gl_account_description) as gl_account_description,
    coalesce(c.account_class, o.account_class) as account_class,
    coalesce(c.category, o.category) as category,
    coalesce(c.intercompany_flag, o.intercompany_flag) as intercompany_flag,
    1 as active
    from cleaned_chart_of_accounts c
    left join original_chart_of_accounts o on
        c.gl_account_id = o.gl_account_id
;
end;
$$
