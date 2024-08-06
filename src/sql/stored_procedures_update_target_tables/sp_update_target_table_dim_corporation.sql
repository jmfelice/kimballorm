CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_dim_corporation()
LANGUAGE plpgsql
AS $$
BEGIN

CALL finance_etl.sp_update_source_table_dim_corporation();

UPDATE finance_dw.dim_corporation SET
    corporation_name = update_old_rows.corporation_name,
    corporation_abbr = update_old_rows.corporation_abbr,
    elimination_branch = update_old_rows.elimination_branch,
    federal_id_number = update_old_rows.federal_id_number,
    active = update_old_rows.active
FROM (
    SELECT
        target.corporation_key,
        source.corporation,
        source.corporation_name,
        source.corporation_abbr,
        source.elimination_branch,
        source.federal_id_number,
        source.active
    FROM finance_dw.dim_corporation AS target
    INNER JOIN finance_etl.dim_corporation_source AS source ON (target.corporation = source.corporation OR (source.corporation IS NULL AND target.corporation IS NULL))
    WHERE
        coalesce(target.corporation_name, '') != coalesce(source.corporation_name, '')
        OR coalesce(target.corporation_abbr, '') != coalesce(source.corporation_abbr, '')
        OR coalesce(target.elimination_branch, 0) != coalesce(source.elimination_branch, 0)
        OR coalesce(target.federal_id_number, 0) != coalesce(source.federal_id_number, 0)
        OR coalesce(target.active, 0) != coalesce(source.active, 0)
) AS update_old_rows
WHERE finance_dw.dim_corporation.corporation_key = update_old_rows.corporation_key
;


INSERT INTO finance_dw.dim_corporation (corporation_key, corporation, corporation_name, corporation_abbr, elimination_branch, federal_id_number, active)
SELECT
    source.corporation_key,
    source.corporation,
    source.corporation_name,
    source.corporation_abbr,
    source.elimination_branch,
    source.federal_id_number,
    source.active
FROM finance_etl.dim_corporation_source AS source
LEFT OUTER JOIN finance_dw.dim_corporation AS target ON (source.corporation = target.corporation OR (source.corporation IS NULL AND target.corporation IS NULL))
WHERE target.corporation_key IS NULL
;


WITH soft_delete_cte AS (
    SELECT target.corporation_key
    FROM finance_dw.dim_corporation AS target
    LEFT OUTER JOIN finance_etl.dim_corporation_source AS source ON (target.corporation = source.corporation OR (source.corporation IS NULL AND target.corporation IS NULL))
    WHERE source.corporation_key IS NULL AND target.active = 1
)

UPDATE finance_dw.dim_corporation SET active = 0 FROM soft_delete_cte WHERE finance_dw.dim_corporation.corporation_key = soft_delete_cte.corporation_key

;
END;
$$
