import pandas as pd
import os
import tempfile
from botocore.exceptions import ClientError
import boto3
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from io import BytesIO


def convert_file_to_parquet(input_file, output_file = None):
    """
    Converts a file to Parquet format.

    Parameters:
    - input_file: str, path to the input file (TXT, XLSX, CSV).
    - output_file: str, path to the output Parquet file. If None, a temporary file will be created.

    Returns:
    - str, path to the output Parquet file.
    """
    # Determine the file extension
    file_extension = os.path.splitext(input_file)[1].lower()

    # Read the file into a DataFrame
    if file_extension == '.csv':
        df = pd.read_csv(input_file)
    elif file_extension == '.xlsx':
        df = pd.read_excel(input_file)
    elif file_extension == '.txt':
        df = pd.read_csv(input_file, delimiter = '\t')  # Assuming tab-delimited TXT files
    else:
        raise ValueError(f"Unsupported file type: {file_extension}")

    # Convert DataFrame to Parquet
    return convert_dataframe_to_parquet(df, output_file)


def convert_dataframe_to_parquet(df, output_file = None):
    """
    Converts a DataFrame to Parquet format.

    Parameters:
    - df: pandas.DataFrame, the DataFrame to convert.
    - output_file: str, path to the output Parquet file. If None, a temporary file will be created.

    Returns:
    - str, path to the output Parquet file.
    """
    # Use a temporary file if output_file is None
    if output_file is None:
        temp_file = tempfile.NamedTemporaryFile(delete = False, suffix = '.parquet')
        output_file = temp_file.name
        temp_file.close()

    # Write the DataFrame to a Parquet file
    df.to_parquet(output_file, engine = 'pyarrow')
    return output_file


def connect_to_s3():
    load_dotenv()
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

    s3_client = boto3.client(
        's3',
        aws_access_key_id = aws_access_key_id,
        aws_secret_access_key = aws_secret_access_key
    )

    return s3_client


def download_file_from_s3(s3_key):
    """
    Downloads a file from an S3 bucket.

    Parameters:
    - bucket_name: str, name of the S3 bucket.
    - s3_key: str, the S3 object key (path in the bucket).
    - local_file_path: str, path where the file will be saved locally.
    - aws_access_key_id: str, your AWS Access Key ID.
    - aws_secret_access_key: str, your AWS Secret Access Key.

    Returns:
    - bool, True if the file was downloaded successfully, False otherwise.
    """
    load_dotenv()
    bucket_name = os.getenv('S3_BUCKET_NAME')

    s3_client = connect_to_s3()
    # temp_file = tempfile.NamedTemporaryFile(delete = False, suffix = '.parquet')

    try:
        buffer = BytesIO()
        s3_client.download_fileobj(bucket_name, s3_key, buffer)
        buffer.seek(0)  # Move to the start of the buffer

        print(f"Successfully downloaded {s3_key} from {bucket_name}")

        # Determine the file type from the S3 key
        file_extension = s3_key.split('.')[-1].lower()

        # Read the buffer into a DataFrame based on file type
        if file_extension == 'csv':
            df = pd.read_csv(buffer)
        elif file_extension in ['xls', 'xlsx']:
            df = pd.read_excel(buffer)
        elif file_extension == 'json':
            df = pd.read_json(buffer)
        else:
            raise ValueError(f"Unsupported file type: {file_extension}")

        print(f"Successfully converted {s3_key} to DataFrame")
        return df

    except NoCredentialsError:
        print("Credentials not available")
    except PartialCredentialsError:
        print("Incomplete credentials provided")
    except Exception as e:
        print(f"Error downloading file from S3: {e}")

    return None


def push_xlsx_to_s3(directory, csv_file):
    csv_path = directory + csv_file
    s3_path = 'finance/' + csv_file

    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(csv_path, 'fisher-redshift', s3_path)
    os.remove(csv_path)



if __name__ == '__main__':
    # s3_key = "finance/flat_file_account_class.csv"
    #
    # s3_client = connect_to_s3()
    # rslt = download_file_from_s3(s3_key)
    # print(rslt)

    push_xlsx_to_s3(
        "C:/Users/jmfel/Desktop/",
        "Book1.csv"
    )
