
CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_dim_branch()
LANGUAGE plpgsql
AS $$
BEGIN

CALL finance_etl.sp_populate_source_table_dim_branch();

INSERT INTO finance_dw.dim_branch (
    branch_key,
    branch,
    branch_abb,
    branch_name,
    branch_type,
    is_branch,
    corporation,
    manager,
    customer_account,
    region,
    zone,
    servicing_warehouse,
    alternate_warehouse,
    service_charge,
    bank_id,
    bank_name,
    bank_minimum_balance,
    street,
    city,
    state,
    zipcode,
    address,
    open_date,
    close_date,
    start_of_first_full_month,
    scd2_start_date,
    scd2_end_date,
    current_flag,
    active
)
SELECT
    source.branch_key,
    source.branch,
    source.branch_abb,
    source.branch_name,
    source.branch_type,
    source.is_branch,
    source.corporation,
    source.manager,
    source.customer_account,
    source.region,
    source.zone,
    source.servicing_warehouse,
    source.alternate_warehouse,
    source.service_charge,
    source.bank_id,
    source.bank_name,
    source.bank_minimum_balance,
    source.street,
    source.city,
    source.state,
    source.zipcode,
    source.address,
    source.open_date,
    source.close_date,
    source.start_of_first_full_month,
    source.scd2_start_date,
    source.scd2_end_date,
    source.current_flag,
    source.active
FROM finance_dw.dim_branch AS target INNER JOIN finance_etl.dim_branch_source AS source ON (target.branch = source.branch OR (source.branch IS NULL AND target.branch IS NULL))
WHERE
    target.current_flag = 1
    AND (
        coalesce(target.branch_abb, '') != coalesce(source.branch_abb, '')
        OR coalesce(target.branch_name, '') != coalesce(source.branch_name, '')
        OR coalesce(target.branch_type, '') != coalesce(source.branch_type, '')
        OR coalesce(target.is_branch, 0) != coalesce(source.is_branch, 0)
        OR coalesce(target.corporation, 0) != coalesce(source.corporation, 0)
        OR coalesce(target.manager, '') != coalesce(source.manager, '')
        OR coalesce(target.customer_account, 0) != coalesce(source.customer_account, 0)
        OR coalesce(target.region, '') != coalesce(source.region, '')
        OR coalesce(target.zone, '') != coalesce(source.zone, '')
        OR coalesce(target.servicing_warehouse, '') != coalesce(source.servicing_warehouse, '')
        OR coalesce(target.alternate_warehouse, '') != coalesce(source.alternate_warehouse, '')
        OR coalesce(target.service_charge, 0.0) != coalesce(source.service_charge, 0.0)
        OR coalesce(target.bank_id, 0) != coalesce(source.bank_id, 0)
        OR coalesce(target.bank_name, '') != coalesce(source.bank_name, '')
        OR coalesce(target.bank_minimum_balance, 0.0) != coalesce(source.bank_minimum_balance, 0.0)
        OR coalesce(target.street, '') != coalesce(source.street, '')
        OR coalesce(target.city, '') != coalesce(source.city, '')
        OR coalesce(target.state, '') != coalesce(source.state, '')
        OR coalesce(target.zipcode, 0) != coalesce(source.zipcode, 0)
        OR coalesce(target.address, '') != coalesce(source.address, '')
        OR coalesce(target.open_date, '1900-01-01') != coalesce(source.open_date, '1900-01-01')
        OR coalesce(target.close_date, '1900-01-01') != coalesce(source.close_date, '1900-01-01')
        OR coalesce(target.start_of_first_full_month, '1900-01-01') != coalesce(source.start_of_first_full_month, '1900-01-01')
        OR coalesce(target.active, 0) != coalesce(source.active, 0)
    )
; 


UPDATE finance_dw.dim_branch SET
    scd2_end_date = dateadd(
        DAY, -1, update_old_rows.scd2_start_date
    ), current_flag = 0
FROM (
    SELECT
        target.branch_key,
        source.branch,
        source.branch_abb,
        source.branch_name,
        source.branch_type,
        source.is_branch,
        source.corporation,
        source.manager,
        source.customer_account,
        source.region,
        source.zone,
        source.servicing_warehouse,
        source.alternate_warehouse,
        source.service_charge,
        source.bank_id,
        source.bank_name,
        source.bank_minimum_balance,
        source.street,
        source.city,
        source.state,
        source.zipcode,
        source.address,
        source.open_date,
        source.close_date,
        source.start_of_first_full_month,
        source.scd2_start_date,
        source.scd2_end_date,
        source.current_flag,
        source.active
    FROM finance_dw.dim_branch AS target INNER JOIN finance_etl.dim_branch_source AS source ON (target.branch = source.branch OR (source.branch IS NULL AND target.branch IS NULL))
    WHERE
        target.current_flag = 1
        AND (
            coalesce(target.branch_abb, '') != coalesce(source.branch_abb, '')
            OR coalesce(target.branch_name, '') != coalesce(source.branch_name, '')
            OR coalesce(target.branch_type, '') != coalesce(source.branch_type, '')
            OR coalesce(target.is_branch, 0) != coalesce(source.is_branch, 0)
            OR coalesce(target.corporation, 0) != coalesce(source.corporation, 0)
            OR coalesce(target.manager, '') != coalesce(source.manager, '')
            OR coalesce(target.customer_account, 0) != coalesce(source.customer_account, 0)
            OR coalesce(target.region, '') != coalesce(source.region, '')
            OR coalesce(target.zone, '') != coalesce(source.zone, '')
            OR coalesce(target.servicing_warehouse, '') != coalesce(source.servicing_warehouse, '')
            OR coalesce(target.alternate_warehouse, '') != coalesce(source.alternate_warehouse, '')
            OR coalesce(target.service_charge, 0.0) != coalesce(source.service_charge, 0.0)
            OR coalesce(target.bank_id, 0) != coalesce(source.bank_id, 0)
            OR coalesce(target.bank_name, '') != coalesce(source.bank_name, '')
            OR coalesce(target.bank_minimum_balance, 0.0) != coalesce(source.bank_minimum_balance, 0.0)
            OR coalesce(target.street, '') != coalesce(source.street, '')
            OR coalesce(target.city, '') != coalesce(source.city, '')
            OR coalesce(target.state, '') != coalesce(source.state, '')
            OR coalesce(target.zipcode, 0) != coalesce(source.zipcode, 0)
            OR coalesce(target.address, '') != coalesce(source.address, '')
            OR coalesce(target.open_date, '1900-01-01') != coalesce(source.open_date, '1900-01-01')
            OR coalesce(target.close_date, '1900-01-01') != coalesce(source.close_date, '1900-01-01')
            OR coalesce(target.start_of_first_full_month, '1900-01-01') != coalesce(source.start_of_first_full_month, '1900-01-01')
            OR coalesce(target.active, 0) != coalesce(source.active, 0)
        )
) AS update_old_rows
WHERE finance_dw.dim_branch.branch_key = update_old_rows.branch_key
; 


INSERT INTO finance_dw.dim_branch (
    branch_key,
    branch,
    branch_abb,
    branch_name,
    branch_type,
    is_branch,
    corporation,
    manager,
    customer_account,
    region,
    zone,
    servicing_warehouse,
    alternate_warehouse,
    service_charge,
    bank_id,
    bank_name,
    bank_minimum_balance,
    street,
    city,
    state,
    zipcode,
    address,
    open_date,
    close_date,
    start_of_first_full_month,
    scd2_start_date,
    scd2_end_date,
    current_flag,
    active
)
SELECT
    source.branch_key,
    source.branch,
    source.branch_abb,
    source.branch_name,
    source.branch_type,
    source.is_branch,
    source.corporation,
    source.manager,
    source.customer_account,
    source.region,
    source.zone,
    source.servicing_warehouse,
    source.alternate_warehouse,
    source.service_charge,
    source.bank_id,
    source.bank_name,
    source.bank_minimum_balance,
    source.street,
    source.city,
    source.state,
    source.zipcode,
    source.address,
    source.open_date,
    source.close_date,
    source.start_of_first_full_month,
    '1900-01-01' AS scd2_start_date,
    source.scd2_end_date,
    source.current_flag,
    source.active
FROM finance_etl.dim_branch_source AS source LEFT OUTER JOIN finance_dw.dim_branch AS target ON (source.branch = target.branch OR (source.branch IS NULL AND target.branch IS NULL))
WHERE target.branch_key IS NULL
; 


WITH soft_delete_cte AS (
    SELECT DISTINCT target.branch_key
    FROM finance_dw.dim_branch AS target
    LEFT OUTER JOIN finance_etl.dim_branch_source AS source ON (target.branch = source.branch OR (source.branch IS NULL AND target.branch IS NULL))
    WHERE target.current_flag = 1 AND target.branch_key IS NULL
)

UPDATE finance_dw.dim_branch SET active = 0 FROM soft_delete_cte WHERE finance_dw.dim_branch.branch_key = soft_delete_cte.branch_key

;
END;
$$
