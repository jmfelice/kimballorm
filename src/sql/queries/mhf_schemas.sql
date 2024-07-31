select
table_name,
substring(table_name, 9, 9) as iseries_schema_name,
substring(table_name,19, 8) as iseries_table_name,
substring(table_name, 12, 4) as year,
substring(table_name, 16, 2) as month,
date_part(
    day,
    last_day(cast(substring(table_name, 12, 4) || '-' || substring(table_name, 16, 2) || '-01' as date))
    ) as day,
last_day(cast(substring(table_name, 12, 4) || '-' || substring(table_name, 16, 2) || '-01' as date)) as period_ending
from svv_tables
where table_catalog = 'fisher_prod'
and table_schema = 'finance_staging'
and table_name like 'iseries_mhf%'
order by period_ending
