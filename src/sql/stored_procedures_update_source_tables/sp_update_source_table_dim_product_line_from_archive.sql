CREATE OR REPLACE PROCEDURE finance_etl.sp_populate_source_table_dim_product_line_from_archive(v_year CHAR(4), v_month CHAR(2))
LANGUAGE plpgsql
AS $$

DECLARE
v_truncate_query VARCHAR(MAX) := '';
v_insert_query VARCHAR(MAX) := '';
v_table_name VARCHAR(30) := 'iseries_mhf' || v_year || v_month || '_reflines';
v_source_date_col TEXT := 'last_day(cast('''|| v_year ||'-'|| v_month ||'-01'' as date))';

BEGIN

v_insert_query = '
INSERT INTO finance_etl.dim_product_line_source
(
    product_line_key,
    product_line,
    product_line_description,
    unit_of_measure,
    SCD2_start_date,
    SCD2_end_date,
    current_flag,
    active
)

 with base as
(
    SELECT
    row_number() over(partition by RILINE order by RILINE) as line_count,
    RILINE as product_line,
    RIDESC as product_line_description,
    RIUOM  as unit_of_measure,
    '|| v_source_date_col || ' as source
    from finance_staging.'|| v_table_name ||'
    where risub = 0
)

select
-1 as product_line_key,
cast(null as INT) as product_line,
''None'' as product_line_description,
''None'' as unit_of_measure,
cast(''1900-01-01'' as date) as SCD2_start_date,
cast(''2999-12-31'' as date) as SCD2_end_date,
0 as current_flag,
0 as active

union all

select
coalesce((select max(product_line_key) from finance_dw.dim_product_line), 0) +
    row_number() over(order by product_line) as product_line_key,
product_line,
product_line_description,
coalesce(unit_of_measure, ''None'') as unit_of_measure,
source as SCD2_start_date,
cast(''2999-12-31'' as date) as SCD2_end_date,
1 as current_flag,
1 as active
from base
where line_count = 1
'
;

EXECUTE v_insert_query;
END;
$$
