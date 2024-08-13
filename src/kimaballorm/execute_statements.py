from .generate_statements import (
    generate_drop_statement,
    generate_truncate_statement,
    generate_create_table_statement,
    generate_call_procedure_statement
)
import time
from concurrent.futures import ThreadPoolExecutor


def truncate_table(entity_orm, engine):
    with engine.begin() as connection:
        truncate_statement = generate_truncate_statement(entity_orm)
        connection.execute(truncate_statement)
    return None


def truncate_tables(table_orms, engine):
    start_time = time.time()

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(truncate_table, table, engine) for table in table_orms]
        for future in futures:
            future.result()  # Wait for all tasks to complete

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Truncating all tables took {execution_time:.4f} seconds.")


def recreate_table(entity_orm, engine):
    with engine.begin() as connection:
        drop_statement = generate_drop_statement(entity_orm)
        create_statement = generate_create_table_statement(entity_orm)
        connection.execute(drop_statement)
        connection.execute(create_statement)
    return None


def execute_stored_procedure(procedure_name, engine, **kwargs):
    with engine.begin() as connection:
        call_procedure = generate_call_procedure_statement(procedure_name, **kwargs)
        connection.execute(call_procedure)
    return None


def process_table(table_orm, engine, **kwargs):
    start_time = time.time()

    table_name = table_orm().get_table_name()
    sp_schema = "finance_etl"
    sp_name = f"{sp_schema}.sp_update_target_table_{table_name}"
    execute_stored_procedure(sp_name, engine, **kwargs)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Processing table {table_name} took {execution_time:.4f} seconds.")


def process_tables(table_orms, engine, **kwargs):
    start_time = time.time()

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_table, table, engine, **kwargs) for table in table_orms]
        for future in futures:
            future.result()  # Wait for all tasks to complete

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Processing all tables took {execution_time:.4f} seconds.")


def process_table_from_archive(table_orm, engine, years, months):
    for i in range(len(years)):
        start_time = time.time()

        params = {"v_year": years[i], "v_month": months[i]}
        table_name = table_orm().get_table_name()
        sp_schema = "finance_etl"
        sp_name = f"{sp_schema}.sp_update_target_table_{table_name}_from_archive"
        execute_stored_procedure(sp_name, engine, **params)

        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Processing table {table_name} took {execution_time:.4f} seconds.")

