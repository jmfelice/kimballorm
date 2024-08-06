from sqlalchemy.orm import sessionmaker
from src.util.connect_to_redshift import connect_to_redshift
from src.kimaballorm.orm import *
from src.integration.statements.execute_statements import (
    get_dates_for_archived_schemas,
    process_table,
    print_timing
)


@print_timing
def rebuild_from_archive(eng):
    tables = [
        DimBranch,
        DimProductLine
    ]

    for table in tables:
        year, month = get_dates_for_archived_schemas(table, eng)
        for i in range(len(year)):
            params = {"v_year": year[i], "v_month": month[i]}

            Session = sessionmaker(bind = engine)
            session = Session()
            with session as sesh:
                process_table(table, sesh, process_source = True, **params)


if __name__ == '__main__':
    engine = connect_to_redshift()
    rebuild_from_archive(engine)
