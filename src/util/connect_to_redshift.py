from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

def connect_to_redshift():
    # Load environment variables from a .env file
    load_dotenv()

    # Read database credentials from environment variables
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    dbname = os.getenv('DB_NAME')

    connection_string = f"redshift+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(connection_string)
