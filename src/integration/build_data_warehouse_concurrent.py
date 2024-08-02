from src.kimaballorm.orm import *
from src.util.connect import connect_to_redshift
from sqlalchemy.orm import sessionmaker
import time
from src.util.read_file import read_file
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import text
from functools import wraps
import pandas as pd


def timing_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Extract the table name from the first argument (table_orm)
        table_orm = args[0]
        table_name = table_orm().get_table_name()

        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Processing table '{table_name}' took {execution_time:.4f} seconds.")
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


@timing_decorator
def process_table(table_orm, session, process_source = True, **kwargs):
    if process_source:
        execute_truncate_source(table_orm, session)
    execute_update_target(table_orm, session, **kwargs)


def process_concurrently(table_orms, eng, process_souce = True, **kwargs):
    Session = sessionmaker(bind=eng)
    session = Session()
    with ThreadPoolExecutor() as executor:
        _ = {executor.submit(process_table, table, session, process_souce, **kwargs): table for table in table_orms}
    session.close()


def get_dates_for_archived_schemas(table_orm, eng):
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


if __name__ == '__main__':
    start_time = time.time()

    engine = connect_to_redshift()

    # dimensions = [
    #     DimAccount,
    #     DimAccountClass,
    #     DimCategory,
    #     DimCalendar,
    #     DimCorporation,
    #     DimIndirectCashFlowCategory
    # ]
    #
    # process_concurrently(dimensions, engine, process_souce = True)

    # degenerate_dimensions = [
    #     DimJournalEntry,
    #     DimJournalDescription
    # ]
    #
    # process_concurrently(degenerate_dimensions, engine, process_souce = False)
    #
    # end_time = time.time()
    # print("Total time: ", end_time - start_time)

    # bridges = [
    #     BridgeCategory,
    #     BridgeIndirectCashFlowCategory,
    #     BridgeMapCashFlow
    # ]
    #
    # process_concurrently(bridges, engine, process_souce = True)

    # facts = [
    #     FactGeneralLedger,
    #     FactAcquisitionCashFlow,
    #     FactIncomeSummary,
    #     FactBalanceSheet,
    #     FactCashFlow
    # ]
    #
    # Session = sessionmaker(bind=engine)
    # session = Session()
    #
    # with session as sesh:
    #     for table in facts:
    #         process_table(table, sesh, True)


    # SCD2_dimensions = [
    #     DimBranch,
    #     DimProductLine
    # ]
    #
    # table_ref = {
    #     "dim_branch": "refstor",
    #     "dim_product_line": "reflines"
    # }
    #
    # for dimension in SCD2_dimensions:
    #     year, month = get_dates_for_archived_schemas(dimension, engine)
    #     year = year
    #     month = month
    #
    #     for i in range(len(year)):
    #         params = {"v_year": year[i], "v_month": month[i]}
    #
    #         Session = sessionmaker(bind = engine)
    #         session = Session()
    #         with session as sesh:
    #             process_table(dimension, sesh, process_source = True, **params)



    end_time = time.time()
    print("Total time: ", end_time - start_time)

