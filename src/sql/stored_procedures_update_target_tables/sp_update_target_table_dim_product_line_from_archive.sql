CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_dim_product_line_from_archive(v_year CHAR(4), v_month CHAR(2))
LANGUAGE plpgsql
AS $$
BEGIN

CALL finance_etl.sp_update_source_table_dim_product_line_from_archive(v_year, v_month);

INSERT INTO finance_dw.dim_product_line (product_line_key, product_line, product_line_description, unit_of_measure, scd2_start_date, scd2_end_date, current_flag, active)
SELECT
    source.product_line_key,
    source.product_line,
    source.product_line_description,
    source.unit_of_measure,
    source.scd2_start_date,
    source.scd2_end_date,
    source.current_flag,
    source.active
FROM finance_dw.dim_product_line AS target
INNER JOIN finance_etl.dim_product_line_source AS source ON (target.product_line = source.product_line OR (source.product_line IS NULL AND target.product_line IS NULL))
WHERE
    target.current_flag = 1
    AND (
        coalesce(target.product_line_description, '') != coalesce(source.product_line_description, '')
        OR coalesce(target.unit_of_measure, '') != coalesce(source.unit_of_measure, '')
        OR coalesce(target.active, 0) != coalesce(source.active, 0)
    )
;


UPDATE finance_dw.dim_product_line SET
    scd2_end_date = dateadd(
        DAY, -1, update_old_rows.scd2_start_date
    ), current_flag = 0
FROM (
    SELECT
        target.product_line_key,
        source.product_line,
        source.product_line_description,
        source.unit_of_measure,
        source.scd2_start_date,
        source.scd2_end_date,
        source.current_flag,
        source.active
    FROM finance_dw.dim_product_line AS target
    INNER JOIN finance_etl.dim_product_line_source AS source ON (target.product_line = source.product_line OR (source.product_line IS NULL AND target.product_line IS NULL))
    WHERE
        target.current_flag = 1
        AND (
            coalesce(target.product_line_description, '') != coalesce(source.product_line_description, '')
            OR coalesce(target.unit_of_measure, '') != coalesce(source.unit_of_measure, '')
            OR coalesce(target.active, 0) != coalesce(source.active, 0)
        )
) AS update_old_rows
WHERE finance_dw.dim_product_line.product_line_key = update_old_rows.product_line_key
;


INSERT INTO finance_dw.dim_product_line (product_line_key, product_line, product_line_description, unit_of_measure, scd2_start_date, scd2_end_date, current_flag, active)
SELECT
    source.product_line_key,
    source.product_line,
    source.product_line_description,
    source.unit_of_measure,
    '1900-01-01' AS scd2_start_date,
    source.scd2_end_date,
    source.current_flag,
    source.active
FROM finance_etl.dim_product_line_source AS source
LEFT OUTER JOIN finance_dw.dim_product_line AS target ON (source.product_line = target.product_line OR (source.product_line IS NULL AND target.product_line IS NULL))
WHERE target.product_line_key IS NULL
;


WITH soft_delete_cte AS (
    SELECT DISTINCT target.product_line_key
    FROM finance_dw.dim_product_line AS target
    LEFT OUTER JOIN finance_etl.dim_product_line_source AS source ON (target.product_line = source.product_line OR (source.product_line IS NULL AND target.product_line IS NULL))
    WHERE target.current_flag = 1 AND target.product_line_key IS NULL
)

UPDATE finance_dw.dim_product_line SET active = 0 FROM soft_delete_cte WHERE finance_dw.dim_product_line.product_line_key = soft_delete_cte.product_line_key

;
END;
$$
