from sqlalchemy.schema import CreateTable, DropTable
from sqlalchemy import text
from sqlalchemy.ext.compiler import compiles
import sqlfluff
import os
import shutil
from datetime import datetime


def generate_crud_statements(target_table):
    source_entity = target_table().get_source_entity()
    sql_statements = target_table().sync_with_source(source_entity)
    return sql_statements


@compiles(DropTable, "redshift")
def _compile_drop_table(element, compiler, **kwargs):
    return compiler.visit_drop_table(element) + " CASCADE"


def generate_drop_statement(target_table):
    table = target_table().get_table()
    return DropTable(table)


def generate_create_table_statement(target_table):
    table = target_table().get_table()
    return CreateTable(table)


def generate_truncate_statement(entity_orm):
    tbl_name = entity_orm().get_table_name()
    schm_name = entity_orm().get_schema_name()
    return f"truncate table {schm_name}.{tbl_name};"


def generate_call_procedure_statement(procedure_name, **kwargs):
    if kwargs:
        params = ", ".join(f"{key}=>{value}" for key, value in kwargs.items())
        proc = f"CALL {procedure_name}({params});"
    else:
        proc = f"CALL {procedure_name}();"

    return proc


def generate_target_table_update_stored_procedure(target_entity, eng, config_path = None):
    source_entity = target_entity().get_source_entity()
    schema_name = source_entity().get_schema_name()
    table_name = target_entity().get_table_name()

    sp_populate_source_table_name = f"{schema_name}.sp_populate_source_table_{table_name}"
    populate_source_table = generate_call_procedure_statement(sp_populate_source_table_name)
    crud_statements = generate_crud_statements(target_entity)
    crud_statements = target_entity().compile_sql(crud_statements, eng)
    crud_statements = [sqlfluff.fix(x, config_path = config_path) for x in crud_statements]

    header = f"""
CREATE OR REPLACE PROCEDURE finance_etl.sp_update_target_table_{table_name}()
LANGUAGE plpgsql
AS $$
BEGIN
"""

    footer = """
;
END;
$$
"""

    proc = (header + "\n" +
        populate_source_table + "\n\n" +
        '; \n\n\n'.join(crud_statements) +
        footer
        )

    return sqlfluff.fix(proc, config_path = config_path)


def save_sql_query(query, file_path, backup_dir):
    # Ensure the backup directory exists
    os.makedirs(backup_dir, exist_ok = True)

    # If the file already exists, create a backup
    if os.path.exists(file_path):
        # Get the file name without the path
        file_name = os.path.basename(file_path)

        # Create a backup file name with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{os.path.splitext(file_name)[0]}_{timestamp}{os.path.splitext(file_name)[1]}"
        backup_path = os.path.join(backup_dir, backup_name)

        # Copy the existing file to the backup location
        shutil.copy2(file_path, backup_path)
        print(f"Backup created: {backup_path}")

    # Write the SQL query to the file
    with open(file_path, 'w') as file:
        file.write(query)
    print(f"SQL query saved to: {file_path}")
