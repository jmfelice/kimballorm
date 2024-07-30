CREATE OR REPLACE PROCEDURE finance_etl.sp_populate_source_table_dim_calendar()
AS $$
BEGIN
    INSERT INTO finance_etl.dim_calendar_source
    (
        DATE_KEY,
        CALENDAR_DATE,
        YEAR,
        QUARTER,
        MONTH,
        NAME_OF_DAY,
        DAY_OF_MONTH,
        DAY_OF_YEAR,
        WEEK,
        DAY_OF_WEEK,
        HOLIDAY,
        WEIGHTED_VALUE,
        DAYS_IN_MONTH,
        NAME_OF_MONTH,
        NAME_OF_MONTH_ABB,
        FIRST_DATE_OF_MONTH,
        FIRST_DATE_OF_YEAR,
        FIRST_DATE_OF_QUARTER,
        FIRST_DATE_OF_LTM,
        PY_DATE,
        PY_YEAR,
        PY_MONTH,
        PM_DATE,
        PM_YEAR,
        PM_MONTH,
        FISCAL_YEAR,
        FISCAL_WEEK,
        DAY_OF_FISCAL_YEAR,
        DAY_FREQUENCY_OF_FISCAL_YEAR,
        FIRST_DATE_OF_FISCAL_WEEK,
        LAST_DATE_OF_FISCAL_WEEK,
        PY_FISCAL_YEAR,
        CS_PY_DAILY_DATE,
        CS_PY_DAILY_FISCAL_YEAR,
        CS_PY_DAILY_FISCAL_WEEK,
        FIRST_DATE_OF_CS_PY_FISCAL_WEEK,
        WEIGHTED_BUSINESS_DAYS_MTD,
        WEIGHTED_BUSINESS_DAYS_QTD,
        WEIGHTED_BUSINESS_DAYS_YTD,
        WEIGHTED_BUSINESS_DAYS_LTM,
        WEIGHTED_BUSINESS_DAYS_WTD,
        active
    )

    SELECT
    -1 AS DATE_KEY,
    cast(NULL as date) AS CALENDAR_DATE,
    0 AS YEAR,
    0 AS QUARTER,
    0 AS MONTH,
    'None' as NAME_OF_DAY,
    0 as DAY_OF_MONTH,
    0 as DAY_OF_YEAR,
    0 AS WEEK,
    0 AS DAY_OF_WEEK,
    'None' as HOLIDAY,
    0 AS WEIGHTED_VALUE,
    0 AS DAYS_IN_MONTH,
    CAST('None' AS CHAR(9)) as NAME_OF_MONTH,
    CAST('None' AS CHAR(3)) as NAME_OF_MONTH_ABB,
    CAST('2999-12-31' AS DATE) AS FIRST_DATE_OF_MONTH,
    CAST('2999-12-31' AS DATE) AS FIRST_DATE_OF_YEAR,
    CAST('2999-12-31' AS DATE) AS FIRST_DATE_OF_QUARTER,
    CAST('2999-12-31' AS DATE) AS FIRST_DATE_OF_LTM,
    CAST('2999-12-31' AS DATE) AS PY_DATE,
    0 AS PY_YEAR,
    0 AS PY_MONTH,
    CAST('2999-12-31' AS DATE) AS PM_DATE,
    0 AS PM_YEAR,
    0 AS PM_MONTH,
    0 AS FISCAL_YEAR,
    0 AS FISCAL_WEEK,
    0 AS DAY_OF_FISCAL_YEAR,
    0 AS DAY_FREQUENCY_OF_FISCAL_YEAR,
    CAST('2999-12-31' AS DATE) AS FIRST_DATE_OF_FISCAL_WEEK,
    CAST('2999-12-31' AS DATE) AS LAST_DATE_OF_FISCAL_WEEK,
    0 AS PY_FISCAL_YEAR,
    CAST('2999-12-31' AS DATE) AS CS_PY_DAILY_DATE,
    0 AS CS_PY_DAILY_FISCAL_YEAR,
    0 AS CS_PY_DAILY_FISCAL_WEEK,
    CAST('2999-12-31' AS DATE) AS FIRST_DATE_OF_CS_PY_FISCAL_WEEK,
    0 as WEIGHTED_BUSINESS_DAYS_MTD,
    0 as WEIGHTED_BUSINESS_DAYS_QTD,
    0 as WEIGHTED_BUSINESS_DAYS_YTD,
    0 as WEIGHTED_BUSINESS_DAYS_LTM,
    0 as WEIGHTED_BUSINESS_DAYS_WTD,
    0 as active

    union all

    select
    cast(REPLACE(CALENDAR_DATE, '-', '') as int) as date_key,
    CALENDAR_DATE,
    YEAR,
    QUARTER,
    MONTH,
    NAME_OF_DAY,
    DAY_OF_MONTH,
    DAY_OF_YEAR,
    WEEK,
    DAY_OF_WEEK,
    HOLIDAY,
    WEIGHTED_VALUE,
    DAYS_IN_MONTH,
    NAME_OF_MONTH,
    NAME_OF_MONTH_ABB,
    FIRST_DATE_OF_MONTH,
    FIRST_DATE_OF_YEAR,
    FIRST_DATE_OF_QUARTER,
    FIRST_DATE_OF_LTM,
    PY_DATE,
    PY_YEAR,
    PY_MONTH,
    PM_DATE,
    PM_YEAR,
    PM_MONTH,
    FISCAL_YEAR,
    FISCAL_WEEK,
    DAY_OF_FISCAL_YEAR,
    DAY_FREQUENCY_OF_FISCAL_YEAR,
    FIRST_DATE_OF_FISCAL_WEEK,
    LAST_DATE_OF_FISCAL_WEEK,
    PY_FISCAL_YEAR,
    CS_PY_DAILY_DATE,
    CS_PY_DAILY_FISCAL_YEAR,
    CS_PY_DAILY_FISCAL_WEEK,
    FIRST_DATE_OF_CS_PY_FISCAL_WEEK,
    WEIGHTED_BUSINESS_DAYS_MTD,
    WEIGHTED_BUSINESS_DAYS_QTD,
    WEIGHTED_BUSINESS_DAYS_YTD,
    WEIGHTED_BUSINESS_DAYS_LTM,
    WEIGHTED_BUSINESS_DAYS_WTD,
    1 as active
    from finance_staging.flat_file_calendar
;

END;
$$ LANGUAGE plpgsql;
