from src.kimaballorm.orm import *
from sqlalchemy.schema import CreateTable, DropTable
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from src.util.connect import connect_to_redshift
import sqlfluff


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
        proc = f"CALL {procedure_name}();"

    return text(proc)


def update_target_table(entity_orm, eng):
    source_entity = entity_orm().get_source_entity()
    schema_name = source_entity().get_schema_name()
    table_name = entity_orm().get_table_name()
    sp_populate_source_table_name = f"{schema_name}.sp_populate_source_table_{table_name}"

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
        f"{schema_name}.sp_populate_source_table_{table_name}"
        f"("
        f"v_year = {year}, "
        f"v_month = {month}"
        f")"
    )

    compiled_statements = generate_crud_statements(entity_orm)
    truncate_statement = generate_truncate_statement(entity_orm)
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


if __name__ == '__main__':
    engine = connect_to_redshift()

    # SCD1_dimensions = [
    #     DimAccount,
    #     DimAccountClass,
    #     DimCategory,
    #     DimCalendar,
    #     DimCorporation,
    #     DimIndirectCashFlowCategory,
    #     DimJournalEntry,
    #     DimJournalDescription
    # ]
    #
    # for dimension in SCD1_dimensions:
    #     # recreate_target_table(dimension, engine)
    #     # recreate_source_table(dimension, engine)
    #     update_target_table(dimension, engine)

    # SCD2_dimensions = [
    #     DimBranch,
    #     DimProductLine
    # ]
    #
    # for dimension in SCD2_dimensions:
    #     recreate_target_table(dimension, engine)
    #     recreate_source_table(dimension, engine)
    #     update_target_table(dimension, engine)

    # bridges = [
    #     BridgeCategory,
    #     BridgeMapCashFlow,
    #     BridgeIndirectCashFlowCategory
    # ]
    #
    # for bridge in bridges:
    #     recreate_target_table(bridge, engine)
    #     recreate_source_table(bridge, engine)
    #     update_target_table(bridge, engine)

    facts = [
        FactGeneralLedger
        # FactBalanceSheet,
        # FactAcquisitionCashFlow,
        # FactCashFlow,
        # FactIncomeSummary
    ]

    for fact in facts:
        # recreate_target_table(fact, engine)
        # recreate_source_table(fact, engine)
        # update_target_table(fact, engine)
        print_crud_statements(fact, engine)

