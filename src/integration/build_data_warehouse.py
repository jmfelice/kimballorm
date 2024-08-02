from src.kimaballorm.orm import *
from src.util.connect import connect_to_redshift
from sqlalchemy.orm import sessionmaker
import time
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import text
from src.util.read_file import read_file
import pandas as pd

from src.integration.update_tables import recreate_source_table, recreate_target_table

table_ref = {
    "dim_branch": "refstor",
    "dim_product_line": "reflines"
}


def execute_update_target(table_orm, session, **kwargs):
    table_name = table_orm().get_table_name()
    if kwargs:
        param_placeholders = ', '.join(["'" + val + "'" for val in params.values()])
        update_statement = f"CALL finance_etl.sp_update_target_table_{table_name}_from_archive({param_placeholders});"
    else:
        update_statement = f"CALL finance_etl.sp_update_target_table_{table_name}();"

    print(update_statement)
    session.execute(text(update_statement))


def execute_truncate_source(table_orm, session):
    source_table_name = table_orm().get_source_entity().get_table_name()
    truncate_source_statement = f"truncate table finance_etl.{source_table_name};"
    session.execute(text(truncate_source_statement))


def process_table(table_orm, session, process_source = True, **kwargs):
    start_time_for_table = time.time()
    if process_source:
        execute_truncate_source(table_orm, session)
    execute_update_target(table_orm, session, **kwargs)
    end_time_for_table = time.time()
    table_completion_time = end_time_for_table - start_time_for_table
    table_name = table_orm().get_table_name()
    print(f"{table_name} completed with a time of: {table_completion_time}")


def process_sequentially(table_orms, eng, process_souce = True, **kwargs):
    Session = sessionmaker(bind=eng)
    session = Session()
    for table in table_orms:
        with session as sesh:
            process_table(table, sesh, process_source = process_souce, **kwargs)




if __name__ == '__main__':
    start_time = time.time()

    engine = connect_to_redshift()
    #######################
    # Dimensions
    #######################
    # dimensions = [
    #     DimAccount,
    #     DimAccountClass,
    #     DimCategory,
    #     DimCalendar,
    #     DimCorporation,
    #     DimIndirectCashFlowCategory
    # ]
    #
    # for table in dimensions:
    #     start_time_for_table = time.time()
    #     execute_truncate_source(table, session)
    #     execute_update_target(table, session)
    #     end_time_for_table = time.time()
    #     print("Table time: ", end_time_for_table - start_time_for_table)

    #######################
    # Degenerate Dimensions
    #######################
    # degenerate_dimensions = [
    #     DimJournalEntry,
    #     DimJournalDescription
    # ]
    #
    # for table in degenerate_dimensions:
    #     start_time_for_table = time.time()
    #     execute_update_target(table, session)
    #     end_time_for_table = time.time()
    #     print("Table time: ", end_time_for_table - start_time_for_table)


    #################
    # SCD2 Dimensions
    #################
    SCD2_dimensions = [
        DimBranch,
        # DimProductLine
    ]

    for dimension in SCD2_dimensions:
        year, month = get_dates_for_archived_schemas(dimension, engine)
        year = year
        month = month

        for i in range(len(year)):
            params = {"v_year": year[i], "v_month": month[i]}

            Session = sessionmaker(bind = engine)
            session = Session()
            with session as sesh:
                process_table(dimension, sesh, process_source = True, **params)



    end_time = time.time()
    print("Total time: ", end_time - start_time)

