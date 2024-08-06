CREATE OR REPLACE PROCEDURE finance_etl.sp_update_source_table_fact_balance_sheet_beginning_balance()
LANGUAGE plpgsql
AS $$

    DECLARE
        query VARCHAR(max);
        beginning_yr INT;

    BEGIN

    drop table if exists ##beginning_balance;
    create table ##beginning_balance
    (
        gl_account_id_key        INT,
        branch_key               INT,
        corporation_key          INT,
        category_key             INT,
        posting_date_key         INT,
        beginning_debit_balance  DECIMAL(20, 8),
        beginning_credit_balance DECIMAL(20, 8),
        beginning_balance        DECIMAL(20, 8)
    );

    WITH first_balance_sheet_date AS
    (
    select
    date_part(year, min(cast('20' || substring(table_name, 11, 2) ||  '-12-01' as date))) -1 as yr
    from SVV_tables
    WHERE table_name LIKE 'iseries_gl%' and table_name not like 'iseries_glflib%'
    )

    , first_january_of_the_general_ledger AS
    (
    select date_part(year, min(glperiod)) as yr from finance_staging.iseries_usrjmflib_arfadtv110
    where date_part(month, glperiod) = 1
    )

    , first_usable_balance_date AS
    (
    select max(yr) as yr from (
        select yr from first_balance_sheet_date union all
        select yr from first_january_of_the_general_ledger
    )
    )

    select yr into beginning_yr from first_usable_balance_date;

    query :=
        '
        insert into ##beginning_balance
        (
            gl_account_id_key       ,
            branch_key              ,
            corporation_key         ,
            category_key            ,
            posting_date_key        ,
            beginning_debit_balance ,
            beginning_credit_balance,
            beginning_balance
        )

        select
        coalesce(a.gl_account_id_key    , -1) as gl_account_id_key,
        coalesce(b.branch_key           , -1) as branch_key,
        coalesce(cp.corporation_key     , -1) as corporation_key,
        coalesce(ct.category_key        , -1) as category_key,
        coalesce(c.date_key             , -1) as posting_date_key,
        sum(rbdr)        AS beginning_debit_balance,
        sum(rbcr)        AS beginning_credit_balance,
        sum(rbdr - rbcr) as beginning_balance
        FROM finance_staging.iseries_gl'|| right(beginning_yr + 1, 2)|| 'lib_arlballt f
        left join finance_dw.dim_account     a  on a.gl_account_id  = f.rbglcd
        left join finance_dw.dim_corporation cp on cp.corporation   = f.rbcorp
        left join finance_dw.dim_category    ct on ct.category      = a.gl_category
        left join finance_dw.dim_calendar    c  on c.calendar_date  = cast(''' || beginning_yr || '-12-31'' as date)
        left join finance_dw.dim_branch      b  on
            b.branch = f.rbbr and
            c.calendar_date between b.SCD2_start_date and b.SCD2_end_date
        where rbmont = 12
        group by
            a.gl_account_id_key,
            b.branch_key       ,
            cp.corporation_key ,
            ct.category_key    ,
            c.date_key
        ';

    EXECUTE query;

END;
$$;
