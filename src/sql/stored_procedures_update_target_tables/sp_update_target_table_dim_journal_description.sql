CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_dim_journal_description()
AS $$

DECLARE
    data_does_not_exist BOOL := true;

BEGIN

data_does_not_exist := (select count(*) < 1 from finance_dw.dim_journal_description limit 1);

if data_does_not_exist then
    INSERT INTO finance_dw.dim_journal_description
    (
        description_key,
        description
    )

    select
    -1 as description_key,
    cast(null as varchar(100)) as description
    ;
    end if;

INSERT INTO finance_dw.dim_journal_description
(
    description_key,
    description
)

with new_records as
(
select cast(f.rbdesc as varchar(100)) as description
from finance_staging.iseries_usrjmflib_arfadtv110 f
left join finance_dw.dim_journal_description j on j.description = cast(f.rbdesc as varchar(100))
where j.description is null
group by cast(f.rbdesc as varchar(100))
)

select
coalesce((select max(description_key) from finance_dw.dim_journal_description), 0) +
    row_number() over(order by description) as description_key,
description
from new_records

;
END;
$$ LANGUAGE plpgsql;
