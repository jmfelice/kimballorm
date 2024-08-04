from src.util.connect import connect_to_redshift
from sqlalchemy.orm import sessionmaker
from src.util.read_file import read_file
import os
import glob


def list_files_with_extension(directory, extension):
    search_pattern = f'*{extension}'
    files_with_extension = glob.glob(search_pattern, root_dir = directory)
    return files_with_extension


def publish_stored_procedure(eng, directory = "../sql/stored_procedures_update_target_tables/"):
    Session = sessionmaker(bind = eng)
    session = Session()
    files = list_files_with_extension(directory, "sql")

    for file_name in files:
        print(f"Publishing Stored Procedure from file: {file_name}")
        full_path = os.path.join(directory, file_name)
        stored_procedure = read_file(full_path)
        with session as sesh:
            sesh.execute(stored_procedure)
            sesh.commit()


if __name__ == '__main__':
    engine = connect_to_redshift()
    publish_stored_procedure(engine)

