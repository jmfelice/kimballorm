CREATE OR REPLACE PROCEDURE finance_etl.sp_populate_source_table_dim_corporation()
AS $$
BEGIN

    INSERT INTO finance_etl.dim_corporation_source
    (
        corporation_key,
        corporation,
        corporation_name,
        corporation_abbr,
        elimination_branch,
        federal_id_number,
        active
    )

    SELECT
    -1 as corporation_key,
    cast(null as INT) as corporation,
    'None' as corporation_name,
    'None' as corporation_abbr,
    -1 as elimination_branch,
    0 as federal_id_number,
    0 as active

    union all

    SELECT
    coalesce((select max(corporation_key) from finance_dw.dim_corporation), 0) +
        row_number() over(order by rpcorp) as corporation_key,
    cast(rpcorp as INT) as corporation,
    cast(rpname as VARCHAR(50)) as corporation_name,
    cast(coalesce(rpabbr, 'None') as VARCHAR(30)) as corporation_abbr,
    cast(rpstor as INT) as elimination_branch,
    cast(rpfed# as INT)  as federal_id_number,
    1 as active
    from finance_staging.iseries_rellib_refcorp

;
END;

$$ LANGUAGE plpgsql;
