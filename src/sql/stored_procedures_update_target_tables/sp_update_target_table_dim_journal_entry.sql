CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_dim_journal_entry()
AS $$

DECLARE
    data_does_not_exist BOOL := true;

BEGIN

data_does_not_exist := (select count(*) < 1 from finance_dw.dim_journal_entry limit 1);

if data_does_not_exist then
    INSERT INTO finance_dw.dim_journal_entry
    (
        journal_entry_id_key,
        journal_entry_id
    )

    select
    -1 as journal_entry_id_key,
    cast(null as numeric(14, 0)) as journal_entry_id
    ;
    end if;

INSERT INTO finance_dw.dim_journal_entry
(
    journal_entry_id_key,
    journal_entry_id
)

with new_records as
(
select cast(f.rbseq as numeric(14, 0)) as journal_entry_id
from finance_staging.iseries_usrjmflib_arfadtv110 f
left join finance_dw.dim_journal_entry j on j.journal_entry_id = cast(f.rbseq as numeric(14, 0))
where j.journal_entry_id is null
group by cast(f.rbseq as numeric(14, 0))
)

select
coalesce((select max(journal_entry_id_key) from finance_dw.dim_journal_entry), 0) +
    row_number() over(order by journal_entry_id) as journal_entry_id_key,
journal_entry_id
from new_records

;
END;
$$ LANGUAGE plpgsql;
