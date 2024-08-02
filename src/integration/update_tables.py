from src.integration.generate_statements import (
    generate_create_table_statement,
    generate_crud_statements,
    generate_drop_statement,
    generate_truncate_statement,
    generate_call_procedure_statement
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text


def update_target_table(entity_orm, eng):
    source_entity = entity_orm().get_source_entity()
    schema_name = source_entity().get_schema_name()
    table_name = entity_orm().get_table_name()
    sp_populate_source_table_name = f"{schema_name}.sp_populate_source_table_{table_name}"

    compiled_statements = generate_crud_statements(entity_orm)
    truncate_statement = text(generate_truncate_statement(source_entity))
    procedure_statement = text(generate_call_procedure_statement(sp_populate_source_table_name))

    session = sessionmaker(bind=eng)
    session = session()

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
        f"{schema_name}.sp_populate_source_table_{table_name}"
        f"("
        f"v_year = {year}, "
        f"v_month = {month}"
        f")"
    )

    compiled_statements = generate_crud_statements(entity_orm)
    truncate_statement = generate_truncate_statement(entity_orm)
    procedure_statement = generate_call_procedure_statement(sp_populate_source_table_name)

    session = sessionmaker(bind=eng)
    session = session()

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

    session = sessionmaker(bind = eng)
    session = session()

    with session as sesh:
        sesh.execute(drop_statement)
        sesh.execute(create_statement)
        sesh.commit()


def recreate_source_table(entity_orm, eng):
    source_entity = entity_orm().get_source_entity()
    drop_statement = generate_drop_statement(source_entity)
    create_statement = generate_create_table_statement(source_entity)

    session = sessionmaker(bind = eng)
    session = session()

    with session as sesh:
        sesh.execute(drop_statement)
        sesh.execute(create_statement)
        sesh.commit()
