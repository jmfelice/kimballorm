CREATE OR REPLACE PROCEDURE finance_etl.sp_update_source_table_fact_balance_sheet()
LANGUAGE plpgsql
AS $$
BEGIN

truncate table finance_etl.fact_balance_sheet_source;
CALL finance_etl.sp_update_source_table_fact_balance_sheet_beginning_balance();

insert into finance_etl.fact_balance_sheet_source
(
    foreign_key_hash,
    measures_hash,
    branch_key,
    gl_account_id_key,
    category_key,
    corporation_key,
    posting_date_key,
    debit_balance,
    credit_balance,
    balance,
    action
)

    WITH all_months as
    (
    /*
    Get all dates between the date of the beginning balance
    and the last date available in the general ledger.
    */
    select date_key as posting_date_key
    from finance_dw.dim_calendar
    where
        day_of_month = days_in_month and
        date_key between
            (select min(posting_date_key) from ##beginning_balance) and
            (select max(posting_date_key) from finance_dw.fact_general_ledger)
    )

    , all_combinations as
    (
    /*
    Get all combinations of branch, account, corporation, and date.
    This ensures that we have no gaps in our dataset.
    */
    select distinct
    branch_key,
    gl_account_id_key,
    corporation_key,
    posting_date_key
    from (
        select branch_key, gl_account_id_key, corporation_key from finance_dw.fact_general_ledger union all
        select branch_key, gl_account_id_key, corporation_key from ##beginning_balance
    ) f
    cross join all_months m
    )

    , re AS
    (
    /*
    The retained earnings balance is required for each branch.
    This is because there's no end of year entry.  Instead, when the year closes
    there is a difference between retained earnings for the prior year and retained
    earnings for the new year.  That gap is equal to the branch's net income.
    */
    select
    branch_key,
    corporation_key,
    c.year + 1 as posting_year,
    sum(sum(f.debit_amount )) over(partition by f.branch_key, f.corporation_key order by c.year rows unbounded preceding) as credit_amount,
    sum(sum(f.credit_amount)) over(partition by f.branch_key, f.corporation_key order by c.year rows unbounded preceding) as debit_amount
    from finance_dw.fact_general_ledger f
    left join finance_dw.dim_account  a on a.gl_account_id_key  = f.gl_account_id_key
    left join finance_dw.dim_calendar c on c.date_key           = f.posting_date_key
    where
        -- date_part(year, last_day(f.glperiod)) + 1 > (select distinct dateadd(year, 1, posting_date) from ##beginning_balance) and
        a.account_class in ('R', 'E')
    group by branch_key, corporation_key, c.year
    )

    , gl AS
    (
    /*
    The monthly general ledger values
    */
    select
    f.branch_key,
    f.gl_account_id_key,
    f.corporation_key,
    f.posting_date_key,
    sum(debit_amount) as debit_amount,
    sum(credit_amount) as credit_amount
    from finance_dw.fact_general_ledger f
    where f.posting_date_key > (select min(posting_date_key) from ##beginning_balance)
    group by f.branch_key, f.gl_account_id_key, f.corporation_key, f.posting_date_key
    )

    , all_data AS
    (
    select
    ac.branch_key,
    ac.gl_account_id_key,
    ac.corporation_key,
    ac.posting_date_key,
    coalesce(bs.beginning_debit_balance , 0) as beginning_debit_balance,
    coalesce(bs.beginning_credit_balance, 0) as beginning_credit_balance,
    case
        when a.account_class in ('R', 'E')
        then sum(coalesce(gl.debit_amount, 0)) over(
            partition by ac.branch_key, ac.gl_account_id_key, ac.corporation_key, left(ac.posting_date_key, 4)
            order by ac.posting_date_key rows unbounded preceding)
        else sum(coalesce(gl.debit_amount, 0)) over(
            partition by ac.branch_key, ac.gl_account_id_key, ac.corporation_key
            order by ac.posting_date_key rows unbounded preceding)
        end  as ytd_debit_amount,
    case
        when a.account_class in ('R', 'E')
        then sum(coalesce(gl.credit_amount, 0)) over(
            partition by ac.branch_key, ac.gl_account_id_key, ac.corporation_key, left(ac.posting_date_key, 4)
            order by ac.posting_date_key rows unbounded preceding)
        else sum(coalesce(gl.credit_amount, 0)) over(
            partition by ac.branch_key, ac.gl_account_id_key, ac.corporation_key
            order by ac.posting_date_key rows unbounded preceding)
        end  as ytd_credit_amount,
    coalesce(re.debit_amount , 0) as re_debit_amount,
    coalesce(re.credit_amount, 0) as re_credit_amount

    from all_combinations ac

    left join finance_dw.dim_account a on
        a.gl_account_id_key = ac.gl_account_id_key

    left join ##beginning_balance bs on
        bs.branch_key        = ac.branch_key        and
        bs.corporation_key   = ac.corporation_key   and
        bs.gl_account_id_key = ac.gl_account_id_key

    left join gl on
        gl.branch_key        = ac.branch_key        and
        gl.corporation_key   = ac.corporation_key   and
        gl.gl_account_id_key = ac.gl_account_id_key and
        gl.posting_date_key  = ac.posting_date_key

    left join re on
        re.branch_key        = ac.branch_key         and
        re.corporation_key   = ac.corporation_key    and
        '31'                 = ac.gl_account_id_key  and
        re.posting_year      = left(ac.posting_date_key, 4)
    )

    , balance_sheet AS
    (
    select
    FNV_HASH(
        CAST(coalesce(f.branch_key       , -1) AS VARCHAR) ||
        CAST(coalesce(f.gl_account_id_key, -1) AS VARCHAR) ||
        CAST(coalesce(c.category_key     , -1) AS VARCHAR) ||
        CAST(coalesce(f.corporation_key  , -1) AS VARCHAR) ||
        CAST(coalesce(f.posting_date_key , -1) AS VARCHAR)
    ) AS foreign_key_hash,
    FNV_HASH(
        CAST(sum(beginning_debit_balance  + ytd_debit_amount  + re_debit_amount ) AS VARCHAR) ||
        CAST(sum(beginning_credit_balance + ytd_credit_amount + re_credit_amount) AS VARCHAR)
    ) AS measures_hash,
    f.branch_key       ,
    f.gl_account_id_key,
    c.category_key     ,
    f.corporation_key  ,
    f.posting_date_key ,
    sum(beginning_debit_balance  + ytd_debit_amount  + re_debit_amount ) as debit_balance,
    sum(beginning_credit_balance + ytd_credit_amount + re_credit_amount) as credit_balance,
    sum(beginning_debit_balance  + ytd_debit_amount  + re_debit_amount ) -
        sum(beginning_credit_balance + ytd_credit_amount + re_credit_amount) as balance
    from all_data f
    left join finance_dw.dim_account a on a.gl_account_id_key = f.gl_account_id_key
    left join finance_dw.dim_category c on c.category = a.gl_category
    group by
        f.branch_key,
        f.gl_account_id_key,
        c.category_key,
        f.corporation_key,
        f.posting_date_key
    )


    SELECT
        source.foreign_key_hash  ,
        source.measures_hash     ,
        source.branch_key        ,
        source.gl_account_id_key ,
        source.category_key      ,
        source.corporation_key   ,
        source.posting_date_key  ,
        source.debit_balance,
        source.credit_balance,
        source.balance,
        'INSERT' as action
    FROM balance_sheet AS source
    LEFT OUTER JOIN finance_dw.fact_balance_sheet AS target ON
        source.foreign_key_hash = target.foreign_key_hash
    WHERE target.foreign_key_hash is null

    union all

    SELECT
        target.foreign_key_hash  ,
        target.measures_hash     ,
        target.branch_key        ,
        target.gl_account_id_key ,
        target.category_key      ,
        target.corporation_key   ,
        target.posting_date_key  ,
        target.debit_balance,
        target.credit_balance,
        target.balance,
        'DELETE' as action
    FROM finance_dw.fact_balance_sheet AS target
    LEFT OUTER JOIN balance_sheet AS source ON
        source.foreign_key_hash = target.foreign_key_hash
    WHERE source.foreign_key_hash is null

    union all

    SELECT
        source.foreign_key_hash               ,
        source.measures_hash                  ,
        source.gl_account_id_key              ,
        source.branch_key                     ,
        source.corporation_key                ,
        source.category_key                   ,
        source.posting_date_key               ,
        source.debit_balance,
        source.credit_balance,
        source.balance,
        'UPDATE' as action
    FROM balance_sheet AS source
    LEFT OUTER JOIN finance_dw.fact_balance_sheet AS target ON
        source.foreign_key_hash = target.foreign_key_hash
    WHERE
        source.foreign_key_hash is not null and
        target.foreign_key_hash is not null and
        source.measures_hash != target.measures_hash

;
END;
$$
