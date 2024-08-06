CREATE OR REPLACE PROCEDURE finance_etl.sp_update_source_table_bridge_map_cash_flow()
AS $$
BEGIN

INSERT INTO finance_etl.bridge_map_cash_flow_source
(
    gl_account_id_key,
    indirect_cash_flow_category_key,
    reverse
)

select
a.gl_account_id_key,
i.indirect_cash_flow_category_key,
f.reverse
from finance_staging.flat_file_cash_flow_map f
left join finance_dw.dim_account a on a.gl_account_id = f.gl_account_id
left join finance_dw.dim_indirect_cash_flow_category i on i.indirect_cash_flow_category = f.indirect_cash_flow_category
where i.indirect_cash_flow_category_key is not null
;
END;

$$ LANGUAGE plpgsql;
