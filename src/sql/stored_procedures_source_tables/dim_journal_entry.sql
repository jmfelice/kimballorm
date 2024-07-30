CREATE OR REPLACE PROCEDURE finance_etl.sp_populate_source_table_dim_journal_entry()
AS $$
BEGIN

INSERT INTO finance_etl.dim_journal_entry_source
(
    journal_entry_id_key,
    journal_entry_id,
    active
)

SELECT
-1 as journal_entry_id_key,
cast(null as DECIMAL(28, 3)) as journal_entry_id,
0 as active

union

SELECT
coalesce((select max(journal_entry_id_key) from finance_dw.dim_journal_entry), 0) +
    row_number() over(order by journal_entry_id) as journal_entry_id_key,
cast(journal_entry_id as DECIMAL(28, 3)) as journal_entry_id,
1 as active
FROM (
    select distinct
    rbseq as journal_entry_id
    from finance_staging.iseries_usrjmflib_arfadtv110
    )
;
END;
$$ LANGUAGE plpgsql;
