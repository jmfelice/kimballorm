from sqlalchemy.schema import CreateTable, DropTable
from sqlalchemy import text


def generate_truncate_statement(entity_orm):
    tbl_name = entity_orm().get_table_name()
    schm_name = entity_orm().get_schema_name()
    return text(f"truncate table {schm_name}.{tbl_name};")


def generate_drop_statement(entity_orm):
    table = entity_orm().get_table()
    return DropTable(table)


def generate_create_table_statement(entity_orm):
    table = entity_orm().get_table()
    return CreateTable(table)


def generate_call_procedure_statement(procedure_name, **kwargs):
    if kwargs:
        params = ', '.join(["'" + val + "'" for val in kwargs.values()])
        proc = f"CALL {procedure_name}({params});"
    else:
        proc = f"CALL {procedure_name}();"
    return text(proc)



