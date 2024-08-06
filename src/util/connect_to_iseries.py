import pandas as pd
import pyodbc
from dotenv import load_dotenv
import os


def iseries_conn():
    load_dotenv()
    dsn = os.getenv('ISERIES_DSN')
    username = os.getenv('ISERIES_USER')
    password = os.getenv('ISERIES_PASSWORD')
    return pyodbc.connect(f"DSN={dsn};UID={username};PWD={password}")


def pull_data_iseries(conn, query):
    return pd.read_sql(query, conn)
