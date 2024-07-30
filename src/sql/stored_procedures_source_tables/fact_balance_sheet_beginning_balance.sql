CREATE OR REPLACE PROCEDURE finance_etl.sp_create_fact_balance_sheet_beginning_balance()
LANGUAGE plpgsql
AS $$

    DECLARE
        query VARCHAR(max);
        beginning_yr INT;

    BEGIN

    drop table if exists ##beginning_balance;
    create table ##beginning_balance
    (
        branch INT,
        gl_account_id VARCHAR(10),
        corporation INT,
        posting_date DATE,
        beginning_debit_balance DECIMAL(20, 8),
        beginning_credit_balance DECIMAL(20, 8),
        beginning_balance DECIMAL(20, 8)
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
            branch,
            gl_account_id,
            corporation,
            posting_date,
            beginning_debit_balance,
            beginning_credit_balance,
            beginning_balance
        )

        select
        rbbr AS branch,
        rbglcd AS gl_account_id,
        rbcorp AS corporation,
        cast(''' || beginning_yr || '-12-31'' as date) as posting_date,
        sum(rbdr) AS beginning_debit_balance,
        sum(rbcr) AS beginning_credit_balance,
        sum(rbdr - rbcr) as beginning_balance
        FROM finance_staging.iseries_gl'|| right(beginning_yr + 1, 2)|| 'lib_arlballt f
        where rbmont = 12
        group by rbglcd, rbbr, rbcorp
        ';

    EXECUTE query;

END;
$$;
