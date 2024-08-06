CREATE OR REPLACE PROCEDURE finance_etl.sp_update_source_table_dim_account_class()
AS $$
BEGIN

    INSERT INTO finance_etl.dim_account_class_source
    (
        account_class_key,
        account_class,
        account_class_description,
        account_class_order,
        active
    )

    select
    -1 as account_class_key,
    cast(null as VARCHAR(1)) as account_class,
    cast(null as VARCHAR(9)) as account_class_description,
    0 as account_class_order,
    0 as active

    union all

    SELECT
    coalesce((select max(account_class_key) from finance_dw.dim_account_class), 0) +
        row_number() over(order by account_class) as account_class_key,
    account_class,
    account_class_description,
    account_class_order,
    1 as active
    from finance_staging.flat_file_account_class
    order by account_class_order

;
END;
$$ LANGUAGE plpgsql;
