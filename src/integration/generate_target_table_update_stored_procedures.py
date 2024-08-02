from src.integration.generate_statements import generate_target_table_update_stored_procedure, save_sql_query
from src.kimaballorm.orm import *
from src.util.connect import connect_to_redshift
import os
import time
from concurrent.futures import ThreadPoolExecutor


config_path = r"C:\Users\jmfel\PycharmProjects\KimballORM\.sqlfluff"
backup_directory = r"C:\Users\jmfel\PycharmProjects\KimballORM\src\sql\backup"
output_directory = r"C:\Users\jmfel\PycharmProjects\KimballORM\src\sql\stored_procedures_update_target_tables"


def generate_target_table_update_stored_procedures(target_orm, config_path):
    start_time = time.time()
    table_name = target_orm().get_table_name()
    file_name = f"sp_update_target_table_{table_name}.sql"
    proc = generate_target_table_update_stored_procedure(target_orm, engine, config_path)
    file_path = os.path.join(output_directory, file_name)
    save_sql_query(proc, file_path, backup_directory)
    end_time = time.time()
    print(f"{table_name} completed in: {start_time - end_time}")


if __name__ == '__main__':
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
    # for table in dimensions:
    #     generate_target_table_update_stored_procedures(table, config_path)
    # scd2_dimensions = [
    #     DimBranch,
    #     DimProductLine
    # ]
    #
    # for table in scd2_dimensions:
    #     generate_target_table_update_stored_procedures(table, config_path)
    #
    # bridges = [
    #     BridgeCategory,
    #     BridgeMapCashFlow,
    #     BridgeIndirectCashFlowCategory
    # ]
    #
    # for table in bridges:
    #     generate_target_table_update_stored_procedures(table, config_path)
    #
    # facts = [
    #     FactCashFlow,
    #     FactBalanceSheet,
    #     FactGeneralLedger,
    #     FactIncomeSummary,
    #     FactAcquisitionCashFlow
    # ]
    #
    # for table in facts:
    #     generate_target_table_update_stored_procedures(table, config_path)
