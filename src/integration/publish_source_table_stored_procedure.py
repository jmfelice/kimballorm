from src.integration.connect import connect_to_redshift
from sqlalchemy.orm import sessionmaker
from src.util.read_file import read_file
import os


def publish_stored_procedure(file_name, eng, directory = "../sql/stored_procedures_source_tables/"):
    print(f"Publishing Stored Procedure from file: {file_name}")

    Session = sessionmaker(bind = eng)
    session = Session()
    full_path = os.path.join(directory, file_name)
    stored_procedure = read_file(full_path)

    with session as sesh:
        sesh.execute(stored_procedure)
        sesh.commit()


if __name__ == '__main__':
    engine = connect_to_redshift()
    publish_stored_procedure("dim_account.sql", engine)
