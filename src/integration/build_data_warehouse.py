from src.kimaballorm.orm import DimAccountClass
from sqlalchemy.schema import CreateTable, DropTable
from sqlalchemy.orm import sessionmaker
from src.integration.connect import connect_to_redshift
import sqlfluff


def generate_crud_statements(target_table, eng):
    source_table = target_table().get_source_table()
    sql_statements = target_table().sync_with_source(source_table)
    statements = target_table().compile_sql(sql_statements, eng)
    return statements


def generate_drop_statement(target_table, eng):
    table = target_table().get_table()
    stmt = DropTable(table).compile(eng, compile_kwargs = {"literal_binds": True})
    return stmt


def generate_create_table_statement(target_table, eng):
    table = target_table().get_table()
    stmt = CreateTable(table).compile(eng, compile_kwargs={"literal_binds": True})
    return stmt


def generate_truncate_statement(entity_orm):
    tbl_name = entity_orm.get_table_name()
    schm_name = entity_orm.get_schema_name()
    return f"truncate table {schm_name}.{tbl_name}"


def generate_call_procedure_statement(procedure_name, **kwargs):
    if kwargs:
        params = ", ".join(f"{key}=>{value}" for key, value in kwargs.items())
        return f"CALL {procedure_name}({params});"
    else:
        return f"CALL {procedure_name}();"


def update_target_table(entity_orm, eng):
    schema_name = entity_orm.get_schema_name()
    table_name = entity_orm.get_table_name()
    sp_populate_source_table_name = f"{schema_name}.sp_populate_source_table_{table_name}"

    compiled_statements = generate_crud_statements(entity_orm, eng)
    truncate_statement = generate_truncate_statement(entity_orm)
    procedure_statement = generate_call_procedure_statement(sp_populate_source_table_name)

    Session = sessionmaker(bind=eng)
    session = Session()

    with session as sesh:
        sesh.execute(truncate_statement)
        sesh.execute(procedure_statement)
        sesh.commit()

        for stmt in compiled_statements:
            sesh.execute(stmt)
            sesh.commit()


def recreate_target_table(entity_orm, eng):
    drop_statement = generate_drop_statement(entity_orm, eng)
    create_statement = generate_create_table_statement(entity_orm, eng)

    Session = sessionmaker(bind = eng)
    session = Session()

    with session as sesh:
        sesh.execute(drop_statement)
        sesh.execute(create_statement)
        sesh.commit()


def recreate_source_table(entity_orm, eng):
    entity_orm = entity_orm.get_source_table()
    drop_statement = generate_drop_statement(entity_orm, eng)
    create_statement = generate_create_table_statement(entity_orm, eng)

    Session = sessionmaker(bind = eng)
    session = Session()

    with session as sesh:
        sesh.execute(drop_statement)
        sesh.execute(create_statement)
        sesh.commit()


def print_crud_statements(entity_orm, eng):
    compiled_statements = generate_crud_statements(entity_orm, eng)
    for statement in compiled_statements:
        print(sqlfluff.fix(statement))


if __name__ == '__main__':
    engine = connect_to_redshift()
    table_orm = DimAccountClass

    # recreate_target_table(table_orm, engine)
    # recreate_source_table(table_orm, engine)
    # update_target_table(table_orm, engine)
    print_crud_statements(table_orm, engine)
