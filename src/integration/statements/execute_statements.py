from sqlalchemy.orm import sessionmaker
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import text
from functools import wraps
from src.util.read_file import read_file
from src.integration.statements.generate_statements import generate_create_table_statement, generate_drop_statement


def print_timing(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Processing took {execution_time:.4f} seconds.")
        return result
    return wrapper


def print_table_name(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        table_orm = args[0]
        table_name = table_orm().get_table_name()
        result = func(*args, **kwargs)
        print(f"Processing table '{table_name}'")
        return result
    return wrapper


def execute_update_target(table_orm, session, **kwargs):
    table_name = table_orm().get_table_name()
    if kwargs:
        param_placeholders = ', '.join(["'" + val + "'" for val in kwargs.values()])
        update_statement = f"CALL finance_etl.sp_update_target_table_{table_name}_from_archive({param_placeholders});"
    else:
        update_statement = f"CALL finance_etl.sp_update_target_table_{table_name}();"
    session.execute(text(update_statement))


def execute_truncate_source(table_orm, session):
    source_table_name = table_orm().get_source_entity().get_table_name()
    truncate_source_statement = f"truncate table finance_etl.{source_table_name};"
    session.execute(text(truncate_source_statement))


@print_timing
@print_table_name
def process_table(table_orm, session, process_source = True, **kwargs):
    if process_source:
        execute_truncate_source(table_orm, session)
    execute_update_target(table_orm, session, **kwargs)


def process_concurrently(table_orms, eng, process_souce = True, **kwargs):
    Session = sessionmaker(bind = eng)
    session = Session()
    with ThreadPoolExecutor() as executor:
        _ = {executor.submit(process_table, table, session, process_souce, **kwargs): table for table in table_orms}
    session.close()


def get_dates_for_archived_schemas(table_orm, eng):
    table_ref = {
        "dim_branch": "refstor",
        "dim_product_line": "reflines"
    }

    table_name = table_orm().get_table_name()
    iseries_table_name = table_ref[table_name]

    file_name = "mhf_schemas.sql"
    directory = "../sql/queries/"
    query = read_file(directory + file_name)

    df = pd.read_sql(text(query), eng)
    df = df.loc[df["iseries_table_name"] == iseries_table_name, ["year", "month"]]
    year = df["year"]
    month = df["month"]
    year = [str(x).ljust(4, '0') for x in year]
    month = [str(x).ljust(2, '0') for x in month]
    return year, month


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
