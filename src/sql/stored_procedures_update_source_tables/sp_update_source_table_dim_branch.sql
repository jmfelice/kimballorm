CREATE OR REPLACE PROCEDURE finance_etl.sp_update_source_table_dim_branch()
LANGUAGE plpgsql
AS $$
BEGIN

    INSERT INTO finance_etl.dim_branch_source
    (
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
    SCD2_start_date,
    SCD2_end_date,
    current_flag,
    active
    )

    with period_ending as
    (
        select current_month_ending as period_ending
        from finance_staging.last_date_of_mhf_schema
    )

    , store_open_dates as
    (
    -- there are 2 tables that contain open dates for stores
    -- it's unclear which is the source of truth
    -- only one table has a defined closing date
    select distinct
        COALESCE(f.rlbran, f2.newbrc) as branch,
        coalesce(f.opendate, f2.opndat) as open_date,
        case when f.closedate = '9999-12-31' then null else f.closedate end as close_date
    from finance_staging.iseries_arflib_arfadtv135 f
    full outer join finance_staging.iseries_arflib_arfnstr f2 on
        coalesce(f.rlbran, -1) = coalesce(f2.newbrc, -1)
    )

    , base as
    (
    -- there are situations where duplicate branches exist in the same table.
    -- for example, mhg202308_refstor, branch 1 is listed 5 times.
    -- there's no way to know which row is the correct one, so we select the first row
    SELECT
    row_number() over(partition by rlbran order by rlbran) as branch_count,
    (select period_ending from period_ending) as source ,
    f.rlbran as branch,
    replace(f.rlabbr, '  ', ' ') as branch_abb,
    case when f.rlbnam = '' then 'None' else replace(f.rlbnam, '  ', ' ') end as branch_name,
    case
        when f.rltyp2 = 'A' and f.rlbryn = 'Y' then 'Branch'
        when f.rltyp2 = 'B'	then 'Branch'
        when f.rltyp2 = 'C'	then 'Corporate'
        when f.rltyp2 = 'I'	then 'Inactive'
        when f.rltyp2 = 'Y'	then 'Elimination'
        when f.rltyp2 = 'W'	then 'Distribution Center'
        when f.rltyp2 = 'S'	then 'Sales'
        when f.rltyp2 = 'M'	then 'Deposits'
        when f.rltyp2 = 'U'	then 'Dummy'
        else 'None'
        end as branch_type,
    case when f.rlbryn = 'Y' then 1 else 0 end as is_branch,
    f.rlcorp as corporation,
    case when f.rlmgr = '' then 'None' else replace(f.rlmgr, '  ', ' ') end as manager,
    case when f.rlacct = 0 then -1 else f.rlacct end as customer_account,
    r.rzdesc as region,
    z.rzdesc as "zone",
    case when f.rlsvwh = '' then 'None' else replace(f.rlsvwh, '  ', ' ') end as servicing_warehouse,
    case when f.rlsecw = '' then 'None' else replace(f.rlsecw, '  ', ' ') end as alternate_warehouse,
    f.rlsvcr as service_charge,
    f.rlidno as bank_id,
    b.runame as bank_name,
    f.rlbmin as bank_minimum_balance,
    f.rlstrt as street,
    f.rlcity as city,
    f.rlstat as state,
    f.rlzip  as zipcode,
    f.rlstrt || ' ' || f.rlcity || ' '|| f.rlstat || ', ' || f.rlzip as address,
    coalesce(o.open_date, cast('1980-01-01' as date)) as open_date,
    coalesce(o.close_date, cast('2999-12-31' as date)) as close_date
    from finance_staging.iseries_rellib_refstor f
    left join finance_staging.iseries_rellib_refregn  r on r.RZREGN = f.rlregn
    left join finance_staging.iseries_rellib_refzones z on z.rzzone = f.rlzone
    left join finance_staging.iseries_rellib_refbanks b on b.ruid# = f.rlidno
    left join store_open_dates                        o on o.branch = f.rlbran
    )

    select
    -1 as branch_key,
    cast(null as INT) as branch,
    'None' as branch_abb,
    'None' as branch_name,
    'None' as branch_type,
    0 as is_branch,
    -1 as corporation,
    'None' as manager,
    -1 as customer_account,
    'None' as region,
    'None' as "zone",
    'None' as servicing_warehouse,
    'None' as alternate_warehouse,
    0 as service_charge,
    -1 as bank_id,
    'None' as bank_name,
    0 as bank_minimum_balance,
    'None' as street,
    'None' as city,
    'None' as state,
    00000 as zipcode,
    'None' as address,
    cast('2999-12-31' as date) as open_date,
    cast('2999-12-31' as date) as close_date,
    cast('2999-12-31' as date) as start_of_first_full_month,
    cast('1900-01-01' as date) as SCD2_start_date,
    cast('2999-01-01' as date) as SCD2_end_date,
    0 as current_flag,
    0 as active

    union all

    select distinct
    coalesce((select max(branch_key) from finance_dw.dim_branch), 0) +
        row_number() over(order by f.branch) as branch_key,
    f.branch,
    f.branch_abb,
    f.branch_name,
    f.branch_type,
    f.is_branch,
    f.corporation,
    f.manager,
    f.customer_account,
    f.region,
    f.zone,
    f.servicing_warehouse,
    f.alternate_warehouse,
    f.service_charge,
    f.bank_id,
    f.bank_name,
    f.bank_minimum_balance,
    f.street,
    f.city,
    f.state,
    f.zipcode,
    f.address,
    f.open_date,
    f.close_date,
    DATEADD(day, 1, last_day(f.open_date)) as start_of_first_full_month,
    source as SCD2_start_date,
    cast('2999-12-31' as date) as SCD2_end_date,
    1 as current_flag,
    1 as active
    from base f
    where f.branch_count = 1
    ;

END;
$$
