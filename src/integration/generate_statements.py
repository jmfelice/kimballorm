from sqlalchemy.schema import CreateTable, DropTable
from sqlalchemy import text
from sqlalchemy.ext.compiler import compiles


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
    return text(f"truncate table {schm_name}.{tbl_name};")


def generate_call_procedure_statement(procedure_name, **kwargs):
    if kwargs:
        params = ", ".join(f"{key}=>{value}" for key, value in kwargs.items())
        proc = f"CALL {procedure_name}({params});"
    else:
        proc = f"CALL {procedure_name}();"

    return text(proc)
