CREATE OR REPLACE PROCEDURE finance_etl.sp_populate_source_table_dim_journal_description()
AS $$
BEGIN

INSERT INTO finance_etl.dim_journal_description_source
(
    description_key,
    description,
    active
)

select
-1 as description_key,
cast(null as varchar(256)) as description,
0 as active

union all

SELECT
coalesce((select max(description_key) from finance_dw.dim_journal_description), 0) +
    row_number() over(order by description) as description_key,
description,
1 as acive
FROM (
    select distinct
    rbdesc as description
    from finance_staging.iseries_usrjmflib_arfadtv110
    )
;
END;
$$ LANGUAGE plpgsql;
