from src.kimaballorm.orm import *
from src.util.connect import connect_to_redshift
from src.integration.update_tables import recreate_source_table, recreate_target_table
from src.integration.generate_statements import generate_create_table_statement

from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.schema import CreateTable, DropTable


if __name__ == '__main__':
    engine = connect_to_redshift()
    recreate_source_table(FactIncomeSummary, engine)
    recreate_target_table(FactIncomeSummary, engine)

    # Session = sessionmaker(bind = engine)
    # session = Session()
    # create_table_stmt = generate_create_table_statement(FactAcquisitionCashFlowSource)
    # with session as sesh:
    #     sesh.execute(create_table_stmt)
    #     sesh.commit()
