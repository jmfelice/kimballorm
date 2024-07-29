CREATE OR REPLACE PROCEDURE finance_etl.sp_populate_source_table_dim_account()
LANGUAGE plpgsql
AS $$
BEGIN

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
        FROM finance_staging.flat_file_gl_account
    )

    , original_chart_of_accounts as
    (
        select
        rgacct as gl_account_id,
        rgname as gl_account_description,
        rgtype as account_class,
        cast(NULL as varchar(50)) as category,
        case when lower(rgname) like '%inter' or lower(rgname) like '%intra%' then 1 else 0 end as intercompany_flag
        from finance_staging.iseries_rellib_refgl
    )

    SELECT top 1
    -1 as gl_account_id_key,
    null as gl_account_id,
    'None' as gl_account_description,
    null as account_class,
    'None' as category,
    0 as intercompany_flag,
    0 as active
    FROM finance_staging.flat_file_gl_account

    union all

    select
    coalesce((select max(gl_account_id_key) from finance_dw.dim_account), 0) +
        row_number() over(order by COALESCE(c.gl_account_id, o.gl_account_id)) as gl_account_id_key,
    COALESCE(c.gl_account_id, o.gl_account_id) as gl_account_id,
    COALESCE(c.gl_account_description, o.gl_account_description) as gl_account_description,
    COALESCE(c.category, o.category) as category,
    COALESCE(c.intercompany_flag, o.intercompany_flag) as intercompany_flag,
    1 as active
    from cleaned_chart_of_accounts c
    left join original_chart_of_accounts o on
        c.gl_account_id = o.gl_account_id
;
END;
$$
