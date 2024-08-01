from src.kimaballorm.orm import *
from src.util.connect import connect_to_redshift
from src.integration.update_tables import (
    recreate_source_table,
    recreate_target_table,
    update_target_table,
    update_target_table_from_archive
    )
import time



if __name__ == '__main__':
    engine = connect_to_redshift()

    SCD1_dimensions = [
        DimAccount,
        DimAccountClass,
        DimCategory,
        DimCalendar,
        DimCorporation,
        DimIndirectCashFlowCategory,
        DimJournalEntry,
        DimJournalDescription
    ]

    for dimension in SCD1_dimensions:
        recreate_target_table(dimension, engine)
        recreate_source_table(dimension, engine)
        update_target_table(dimension, engine)
        # print_crud_statements(dimension, engine)

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

    # facts = [
        # FactGeneralLedger
        # FactBalanceSheet,
        # FactAcquisitionCashFlow,
        # FactCashFlow,
        # FactIncomeSummary
    # ]

    # for fact in facts:
        # recreate_target_table(fact, engine)
        # recreate_source_table(fact, engine)
        # update_target_table(fact, engine)
        # print_crud_statements(fact, engine)

