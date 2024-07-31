from src.util.connect import connect_to_redshift
from src.util.read_file import read_file
import pandas as pd
from sqlalchemy import text

pd.set_option('display.max_columns', None)


# file_name = "dim_product_line.sql"
# file_name = "dim_account.sql"
file_name = "mhf_schemas.sql"

if __name__ == '__main__':
    directory = "../sql/queries/"
    query = read_file(directory + file_name)

    engine = connect_to_redshift()
    df = pd.read_sql(text(query), engine)
    print(df)
