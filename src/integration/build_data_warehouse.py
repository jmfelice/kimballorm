from src.integration.statements.execute_statements import process_concurrently, print_timing
from src.util.connect_to_redshift import connect_to_redshift
from src.kimaballorm.orm import *


@print_timing
def build_data_warehouse(eng):
    # dimensions = [
    #     DimAccount,
    #     DimAccountClass,
    #     DimCategory,
    #     DimCalendar,
    #     DimCorporation,
    #     DimIndirectCashFlowCategory,
    #     DimBranch,
    #     DimProductLine
    # ]
    #
    # degenerate_dimensions = [
    #     DimJournalEntry,
    #     DimJournalDescription
    # ]
    #
    # bridges = [
    #     BridgeCategory,
    #     BridgeIndirectCashFlowCategory,
    #     BridgeMapCashFlow
    # ]
    #
    # facts = [
    #     FactGeneralLedger,
    #     FactAcquisitionCashFlow
    # ]

    dependent_facts = [
        # FactIncomeSummary,
        # FactBalanceSheet,
        FactCashFlow
    ]

    # process_concurrently(dimensions, eng, process_souce = True)
    # process_concurrently(degenerate_dimensions, eng, process_souce = False)
    # process_concurrently(bridges, eng, process_souce = True)
    # process_concurrently(facts, eng, process_souce = True)
    process_concurrently(dependent_facts, eng, process_souce = True)


if __name__ == '__main__':
    engine = connect_to_redshift()
    build_data_warehouse(engine)
