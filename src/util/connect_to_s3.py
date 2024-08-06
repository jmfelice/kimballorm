from dotenv import load_dotenv
import os
import pandas as pd
import boto3
import tempfile


def push_xlsx_to_s3(directory, xlsx_file, sheet_name = 0):
    csv_file = 'flat_file_' + xlsx_file.replace('.xlsx', '')
    if sheet_name != 0:
        csv_file = csv_file + '_' + sheet_name
    csv_file = csv_file + '.csv'
    xlsx_path = directory + xlsx_file
    csv_path = directory + csv_file
    s3_path = 'finance/' + csv_file

    df = pd.read_excel(xlsx_path, sheet_name = sheet_name)
    df.to_csv(csv_path, index = False)

    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(csv_path, 'fisher-redshift', s3_path)
    os.remove(csv_path)


def push_csv_to_s3(df, target_file):
    temp_file = tempfile.TemporaryFile()
    df.to_csv(temp_file, index = False)
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(temp_file, 'fisher-redshift', target_file)


def push_data_from_s3_to_redshift(
    user,
    source_table,
    target_schema,
    target_table,
    statement_name,
    create_tbl_statement = None):


    s3_bucket_name = os.getenv('S3_BUCKET_NAME')
    s3_uri = os.getenv('S3_URI')
    s3_iam = os.getenv('S3_IAM')
    s3_region = os.getenv('S3_REGION')

    port = os.getenv('DB_PORT')
    dbname = os.getenv('DB_NAME')


    client = boto3.client('redshift-data', region_name = 'us-east-1')

    # s3_loc = f's3://fisher-redshift/finance/{source_table}'
    # redshift_loc = 'arn:aws:iam::974575226261:role/us-east-1-974575226261-fisher-production'

    sql_statements = [
        f'DROP TABLE {target_schema}.{target_table}',
        create_tbl_statement,
        f'''
		COPY fisher_prod.{target_schema}.{target_table} FROM 's3://fisher-redshift/finance/{source_table}' IAM_ROLE
		'arn:aws:iam::974575226261:role/us-east-1-974575226261-fisher-production' FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'us-east-1'
		'''
    ]

    while None in sql_statements: sql_statements.remove(None)

    client.batch_execute_statement(
        ClusterIdentifier = 'fisher-production',
        Database = 'fisher_prod',
        DbUser = user,
        Sqls = sql_statements,
        StatementName = statement_name,
        WithEvent = False
    )
