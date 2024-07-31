from src.kimaballorm.orm import *
from sqlalchemy.schema import CreateTable, DropTable
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from src.util.connect import connect_to_redshift
from src.util.read_file import read_file
import sqlfluff
import pandas as pd


def generate_crud_statements(target_table):
    source_entity = target_table().get_source_entity()
    sql_statements = target_table().sync_with_source(source_entity)
    return sql_statements


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
        proc = f"CALL {procedure_name};"

    return text(proc)


def update_target_table(entity_orm, eng):
    source_entity = entity_orm().get_source_entity()
    schema_name = source_entity().get_schema_name()
    table_name = entity_orm().get_table_name()
    sp_populate_source_table_name = f"{schema_name}.sp_populate_source_table_{table_name}()"

    compiled_statements = generate_crud_statements(entity_orm)
    truncate_statement = generate_truncate_statement(source_entity)
    procedure_statement = generate_call_procedure_statement(sp_populate_source_table_name)

    Session = sessionmaker(bind=eng)
    session = Session()

    with session as sesh:
        sesh.execute(truncate_statement)
        sesh.execute(procedure_statement)
        sesh.commit()

        for stmt in compiled_statements:
            sesh.execute(stmt, execution_options={"synchronize_session": False})
            sesh.commit()


def update_target_table_from_archive(entity_orm, eng, year = None, month = None):
    source_entity = entity_orm().get_source_entity()
    schema_name = source_entity().get_schema_name()
    table_name = entity_orm().get_table_name()
    sp_populate_source_table_name = (
        f"{schema_name}.sp_populate_source_table_{table_name}_from_archive"
        f"("
        f"'{year}', "
        f"'{month}'"
        f")"
    )

    compiled_statements = generate_crud_statements(entity_orm)
    truncate_statement = generate_truncate_statement(source_entity)
    procedure_statement = generate_call_procedure_statement(sp_populate_source_table_name)

    Session = sessionmaker(bind=eng)
    session = Session()

    with session as sesh:
        sesh.execute(truncate_statement)
        sesh.execute(procedure_statement)
        sesh.commit()

        for stmt in compiled_statements:
            sesh.execute(stmt, execution_options={"synchronize_session": False})
            sesh.commit()


def recreate_target_table(entity_orm, eng):
    drop_statement = generate_drop_statement(entity_orm)
    create_statement = generate_create_table_statement(entity_orm)

    Session = sessionmaker(bind = eng)
    session = Session()

    with session as sesh:
        sesh.execute(drop_statement)
        sesh.execute(create_statement)
        sesh.commit()


def recreate_source_table(entity_orm, eng):
    source_entity = entity_orm().get_source_entity()
    drop_statement = generate_drop_statement(source_entity)
    create_statement = generate_create_table_statement(source_entity)

    Session = sessionmaker(bind = eng)
    session = Session()

    with session as sesh:
        sesh.execute(drop_statement)
        sesh.execute(create_statement)
        sesh.commit()


def print_crud_statements(entity_orm, eng):
    compiled_statements = generate_crud_statements(entity_orm)
    printable_statements = entity_orm.compile_sql(eng, compiled_statements)
    for statement in printable_statements:
        print(sqlfluff.fix(statement))


def print_drop_statement(entity_orm, eng):
    drop_statement = generate_drop_statement(entity_orm)
    printable_statements = entity_orm.compile_sql(eng, drop_statement)
    print(sqlfluff.fix(printable_statements))


def print_create_statement(entity_orm, eng):
    create_statement = generate_create_table_statement(entity_orm)
    printable_statements = entity_orm.compile_sql(eng, create_statement)
    print(sqlfluff.fix(printable_statements))


def get_dates_for_archived_schemas(iseries_table_name, eng):
    file_name = "mhf_schemas.sql"
    directory = "../sql/queries/"
    query = read_file(directory + file_name)

    df = pd.read_sql(text(query), eng)
    df = df.loc[df["iseries_table_name"] == iseries_table_name, ["year", "month"]]
    return df["year"], df["month"]


if __name__ == '__main__':
    engine = connect_to_redshift()

    year, month = get_dates_for_archived_schemas("reflines", engine)
    year = [str(x).ljust(4, '0') for x in year]
    month = [str(x).ljust(2, '0') for x in month]

    year = year[43]
    month = month[43]

    SCD2_dimensions = [
        # DimBranch,
        DimProductLine
    ]

    for dimension in SCD2_dimensions:
        # recreate_target_table(dimension, engine)
        # recreate_source_table(dimension, engine)
        update_target_table_from_archive(dimension, engine, year, month)
        # print_crud_statements(dimension, engine)
