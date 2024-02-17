import os
import sys
import boto3
import botocore
# import logging
from pathlib import Path
from dotenv import load_dotenv
from prefect import get_run_logger, flow, task


# logging.basicConfig(level=logging.INFO)

env_filepath = Path(__file__).parent.parent.resolve().joinpath('config', '.env')

# print(env_filepath)

load_dotenv(env_filepath)

AWS_REGION = os.getenv('aws_region')
AWS_ACCESS_KEY_ID = os.getenv('aws_access_key_id')
AWS_SECRET_ACCESS_KEY = os.getenv('aws_secret_access_key')
BUCKET_NAME = os.getenv('aws_bucket_name')
TRANSFORMED_BUCKET_NAME = os.getenv('transformed_bucket_name')


@task(retries=3, retry_delay_seconds=5, log_prints=True)
def connect_to_s3():
    try:
        logging = get_run_logger()
        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            # region_name=AWS_REGION
        )
        conn = session.client('s3')

        conn.head_bucket(Bucket='extract-load-s3-redshift')

        logging.info('Connection successful')
        return conn
    except Exception as e:
        logging.error(f'Connection failed: {e}', exc_info=True)
        return None


@task
def create_bucket_if_not_exist(conn, BUCKET_NAME):
    try:
        logging = get_run_logger()
        conn.head_bucket(Bucket=BUCKET_NAME)
        logging.info(f'The bucket {BUCKET_NAME} exists already.')
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            logging.info('Bucket does not exit')
            conn.create_bucket(
                Bucket=BUCKET_NAME,
                # CreateBucketConfiguration={'LocationConstraint': AWS_REGION}
            )
            logging.info('New Bucket Created!')


@task
def upload_to_s3(conn, file_path, bucket, object_name):
    logging = get_run_logger()
    if object_name is None:
        object_name = os.path.basename(file_path)
    try:
        conn.upload_file(file_path, bucket, object_name)
        logging.info('File Successfully uploaded!')
    except FileNotFoundError:
        logging.error('File Not Found')    


@flow
def main_s3():
    Bucket = BUCKET_NAME
    file_path = Path(__file__).parent.parent.resolve().joinpath('data', 'output', 'rick.csv')

    conn = connect_to_s3()
    create_bucket_if_not_exist(conn, Bucket)
    upload_to_s3(conn, file_path, Bucket, object_name=None )
    
    Bucket = TRANSFORMED_BUCKET_NAME
    create_bucket_if_not_exist(conn, Bucket)
    



if __name__ == '__main__':
    main_s3()
