CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_dim_calendar()
LANGUAGE plpgsql
AS $$
BEGIN

CALL finance_etl.sp_update_source_table_dim_calendar();

UPDATE finance_dw.dim_calendar SET
    year = update_old_rows.year,
    quarter = update_old_rows.quarter,
    month = update_old_rows.month,
    name_of_day = update_old_rows.name_of_day,
    day_of_month = update_old_rows.day_of_month,
    day_of_year = update_old_rows.day_of_year,
    week = update_old_rows.week,
    day_of_week = update_old_rows.day_of_week,
    holiday = update_old_rows.holiday,
    weighted_value = update_old_rows.weighted_value,
    days_in_month = update_old_rows.days_in_month,
    name_of_month = update_old_rows.name_of_month,
    name_of_month_abb = update_old_rows.name_of_month_abb,
    first_date_of_month = update_old_rows.first_date_of_month,
    first_date_of_year = update_old_rows.first_date_of_year,
    first_date_of_quarter = update_old_rows.first_date_of_quarter,
    first_date_of_ltm = update_old_rows.first_date_of_ltm,
    py_date = update_old_rows.py_date,
    py_year = update_old_rows.py_year,
    py_month = update_old_rows.py_month,
    pm_date = update_old_rows.pm_date,
    pm_year = update_old_rows.pm_year,
    pm_month = update_old_rows.pm_month,
    fiscal_year = update_old_rows.fiscal_year,
    fiscal_week = update_old_rows.fiscal_week,
    day_of_fiscal_year = update_old_rows.day_of_fiscal_year,
    day_frequency_of_fiscal_year = update_old_rows.day_frequency_of_fiscal_year,
    first_date_of_fiscal_week = update_old_rows.first_date_of_fiscal_week,
    last_date_of_fiscal_week = update_old_rows.last_date_of_fiscal_week,
    py_fiscal_year = update_old_rows.py_fiscal_year,
    cs_py_daily_date = update_old_rows.cs_py_daily_date,
    cs_py_daily_fiscal_year = update_old_rows.cs_py_daily_fiscal_year,
    cs_py_daily_fiscal_week = update_old_rows.cs_py_daily_fiscal_week,
    first_date_of_cs_py_fiscal_week = update_old_rows.first_date_of_cs_py_fiscal_week,
    weighted_business_days_mtd = update_old_rows.weighted_business_days_mtd,
    weighted_business_days_qtd = update_old_rows.weighted_business_days_qtd,
    weighted_business_days_ytd = update_old_rows.weighted_business_days_ytd,
    weighted_business_days_ltm = update_old_rows.weighted_business_days_ltm,
    weighted_business_days_wtd = update_old_rows.weighted_business_days_wtd,
    active = update_old_rows.active
FROM (
    SELECT
        target.date_key,
        source.calendar_date,
        source.year,
        source.quarter,
        source.month,
        source.name_of_day,
        source.day_of_month,
        source.day_of_year,
        source.week,
        source.day_of_week,
        source.holiday,
        source.weighted_value,
        source.days_in_month,
        source.name_of_month,
        source.name_of_month_abb,
        source.first_date_of_month,
        source.first_date_of_year,
        source.first_date_of_quarter,
        source.first_date_of_ltm,
        source.py_date,
        source.py_year,
        source.py_month,
        source.pm_date,
        source.pm_year,
        source.pm_month,
        source.fiscal_year,
        source.fiscal_week,
        source.day_of_fiscal_year,
        source.day_frequency_of_fiscal_year,
        source.first_date_of_fiscal_week,
        source.last_date_of_fiscal_week,
        source.py_fiscal_year,
        source.cs_py_daily_date,
        source.cs_py_daily_fiscal_year,
        source.cs_py_daily_fiscal_week,
        source.first_date_of_cs_py_fiscal_week,
        source.weighted_business_days_mtd,
        source.weighted_business_days_qtd,
        source.weighted_business_days_ytd,
        source.weighted_business_days_ltm,
        source.weighted_business_days_wtd,
        source.active
    FROM finance_dw.dim_calendar AS target
    INNER JOIN finance_etl.dim_calendar_source AS source ON (target.calendar_date = source.calendar_date OR (source.calendar_date IS NULL AND target.calendar_date IS NULL))
    WHERE
        coalesce(target.year, 0) != coalesce(source.year, 0)
        OR coalesce(target.quarter, 0) != coalesce(source.quarter, 0)
        OR coalesce(target.month, 0) != coalesce(source.month, 0)
        OR coalesce(target.name_of_day, '') != coalesce(source.name_of_day, '')
        OR coalesce(target.day_of_month, 0) != coalesce(source.day_of_month, 0)
        OR coalesce(target.day_of_year, 0) != coalesce(source.day_of_year, 0)
        OR coalesce(target.week, 0) != coalesce(source.week, 0)
        OR coalesce(target.day_of_week, 0) != coalesce(source.day_of_week, 0)
        OR coalesce(target.holiday, '') != coalesce(source.holiday, '')
        OR coalesce(target.weighted_value, 0.0) != coalesce(source.weighted_value, 0.0)
        OR coalesce(target.days_in_month, 0) != coalesce(source.days_in_month, 0)
        OR coalesce(target.name_of_month, '') != coalesce(source.name_of_month, '')
        OR coalesce(target.name_of_month_abb, '') != coalesce(source.name_of_month_abb, '')
        OR coalesce(target.first_date_of_month, '1900-01-01') != coalesce(source.first_date_of_month, '1900-01-01')
        OR coalesce(target.first_date_of_year, '1900-01-01') != coalesce(source.first_date_of_year, '1900-01-01')
        OR coalesce(target.first_date_of_quarter, '1900-01-01') != coalesce(source.first_date_of_quarter, '1900-01-01')
        OR coalesce(target.first_date_of_ltm, '1900-01-01') != coalesce(source.first_date_of_ltm, '1900-01-01')
        OR coalesce(target.py_date, '1900-01-01') != coalesce(source.py_date, '1900-01-01')
        OR coalesce(target.py_year, 0) != coalesce(source.py_year, 0)
        OR coalesce(target.py_month, 0) != coalesce(source.py_month, 0)
        OR coalesce(target.pm_date, '1900-01-01') != coalesce(source.pm_date, '1900-01-01')
        OR coalesce(target.pm_year, 0) != coalesce(source.pm_year, 0)
        OR coalesce(target.pm_month, 0) != coalesce(source.pm_month, 0)
        OR coalesce(target.fiscal_year, 0) != coalesce(source.fiscal_year, 0)
        OR coalesce(target.fiscal_week, 0) != coalesce(source.fiscal_week, 0)
        OR coalesce(target.day_of_fiscal_year, 0) != coalesce(source.day_of_fiscal_year, 0)
        OR coalesce(target.day_frequency_of_fiscal_year, 0) != coalesce(source.day_frequency_of_fiscal_year, 0)
        OR coalesce(target.first_date_of_fiscal_week, '1900-01-01') != coalesce(source.first_date_of_fiscal_week, '1900-01-01')
        OR coalesce(target.last_date_of_fiscal_week, '1900-01-01') != coalesce(source.last_date_of_fiscal_week, '1900-01-01')
        OR coalesce(target.py_fiscal_year, 0) != coalesce(source.py_fiscal_year, 0)
        OR coalesce(target.cs_py_daily_date, '1900-01-01') != coalesce(source.cs_py_daily_date, '1900-01-01')
        OR coalesce(target.cs_py_daily_fiscal_year, 0) != coalesce(source.cs_py_daily_fiscal_year, 0)
        OR coalesce(target.cs_py_daily_fiscal_week, 0) != coalesce(source.cs_py_daily_fiscal_week, 0)
        OR coalesce(target.first_date_of_cs_py_fiscal_week, '1900-01-01') != coalesce(source.first_date_of_cs_py_fiscal_week, '1900-01-01')
        OR coalesce(target.weighted_business_days_mtd, 0.0) != coalesce(source.weighted_business_days_mtd, 0.0)
        OR coalesce(target.weighted_business_days_qtd, 0.0) != coalesce(source.weighted_business_days_qtd, 0.0)
        OR coalesce(target.weighted_business_days_ytd, 0.0) != coalesce(source.weighted_business_days_ytd, 0.0)
        OR coalesce(target.weighted_business_days_ltm, 0.0) != coalesce(source.weighted_business_days_ltm, 0.0)
        OR coalesce(target.weighted_business_days_wtd, 0.0) != coalesce(source.weighted_business_days_wtd, 0.0)
        OR coalesce(target.active, 0) != coalesce(source.active, 0)
) AS update_old_rows
WHERE finance_dw.dim_calendar.date_key = update_old_rows.date_key
;


INSERT INTO finance_dw.dim_calendar (
    date_key,
    calendar_date,
    year,
    quarter,
    month,
    name_of_day,
    day_of_month,
    day_of_year,
    week,
    day_of_week,
    holiday,
    weighted_value,
    days_in_month,
    name_of_month,
    name_of_month_abb,
    first_date_of_month,
    first_date_of_year,
    first_date_of_quarter,
    first_date_of_ltm,
    py_date,
    py_year,
    py_month,
    pm_date,
    pm_year,
    pm_month,
    fiscal_year,
    fiscal_week,
    day_of_fiscal_year,
    day_frequency_of_fiscal_year,
    first_date_of_fiscal_week,
    last_date_of_fiscal_week,
    py_fiscal_year,
    cs_py_daily_date,
    cs_py_daily_fiscal_year,
    cs_py_daily_fiscal_week,
    first_date_of_cs_py_fiscal_week,
    weighted_business_days_mtd,
    weighted_business_days_qtd,
    weighted_business_days_ytd,
    weighted_business_days_ltm,
    weighted_business_days_wtd,
    active
)
SELECT
    source.date_key,
    source.calendar_date,
    source.year,
    source.quarter,
    source.month,
    source.name_of_day,
    source.day_of_month,
    source.day_of_year,
    source.week,
    source.day_of_week,
    source.holiday,
    source.weighted_value,
    source.days_in_month,
    source.name_of_month,
    source.name_of_month_abb,
    source.first_date_of_month,
    source.first_date_of_year,
    source.first_date_of_quarter,
    source.first_date_of_ltm,
    source.py_date,
    source.py_year,
    source.py_month,
    source.pm_date,
    source.pm_year,
    source.pm_month,
    source.fiscal_year,
    source.fiscal_week,
    source.day_of_fiscal_year,
    source.day_frequency_of_fiscal_year,
    source.first_date_of_fiscal_week,
    source.last_date_of_fiscal_week,
    source.py_fiscal_year,
    source.cs_py_daily_date,
    source.cs_py_daily_fiscal_year,
    source.cs_py_daily_fiscal_week,
    source.first_date_of_cs_py_fiscal_week,
    source.weighted_business_days_mtd,
    source.weighted_business_days_qtd,
    source.weighted_business_days_ytd,
    source.weighted_business_days_ltm,
    source.weighted_business_days_wtd,
    source.active
FROM finance_etl.dim_calendar_source AS source
LEFT OUTER JOIN finance_dw.dim_calendar AS target ON (source.calendar_date = target.calendar_date OR (source.calendar_date IS NULL AND target.calendar_date IS NULL))
WHERE target.date_key IS NULL
;


WITH soft_delete_cte AS (
    SELECT target.date_key
    FROM finance_dw.dim_calendar AS target
    LEFT OUTER JOIN finance_etl.dim_calendar_source AS source ON (target.calendar_date = source.calendar_date OR (source.calendar_date IS NULL AND target.calendar_date IS NULL))
    WHERE source.date_key IS NULL AND target.active = 1
)

UPDATE finance_dw.dim_calendar SET active = 0 FROM soft_delete_cte WHERE finance_dw.dim_calendar.date_key = soft_delete_cte.date_key

;
END;
$$
