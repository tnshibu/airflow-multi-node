import logging
import os

from airflow.hooks import S3_hook
import boto3
import fnmatch

def get_s3_hook():
    return S3_hook.S3Hook(aws_conn_id='s3_airflow')


def download_csv_files(bucket_name, destination):
    s3_client = boto3.client('s3')
    s3_bucket = boto3.resource('s3').Bucket(bucket_name)
    csv_files_list = []
    for file in s3_bucket.objects.all():
        if '.csv' in file.key:
            csv_files_list.append(file.key)
    for csv_file in csv_files_list:
        dest_file = os.path.join(destination, csv_file)
        try:
            logging.info(
                f'Downloading {csv_file} from S3 Bucket {bucket_name}')
            s3_client.download_file(bucket_name, csv_file, dest_file)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                logging.error("The object does not exist.")
            else:
                raise


def upload_to_s3(bucket_name, dir_name, bucket_folder=None, **kwargs):
    s3_hook = get_s3_hook()
    files_to_upload = file_helper.get_files_from_dir(dir_name, '*.csv')
    for each_csv in files_to_upload:
        s3_key = os.path.basename(each_csv)
        if bucket_folder:
            s3_key = f'{bucket_folder}/{s3_key}'
        # Check If file exists in S3 already
        if s3_hook.check_for_key(s3_key, bucket_name):
            logging.info(f'File {s3_key} already exists.. Deleting before uploading')
            s3_hook.delete_objects(bucket_name, s3_key)
        logging.info(f'Uploading {s3_key} to {bucket_name}..')
        s3_hook.load_file(each_csv, s3_key, bucket_name) 
        
def get_file_list(bucket_name, file_pattern):
    s3_hook = get_s3_hook()
    file_list = s3_hook.list_keys(bucket_name, file_pattern)
    # logging.info(file_list)
    return file_list

def remove_files(bucket_name, dir_path='', file_pattern='', **kwargs):
    s3_hook = get_s3_hook()
    file_list = s3_hook.list_keys(bucket_name, dir_path)
    print(file_list)
    filtered_list = [x for x in file_list if file_pattern in x]
    print(filtered_list)
    s3_hook.delete_objects(bucket_name, filtered_list)

if __name__ == "__main__":
#     # download_csv_files('pchenni-airflow-poc',
#     #                    '/usr/local/airflow/data/')
    files_list = get_file_list('ofss-compute-aws', 'market_data/in/')
    re_patern = 'ticker*20200508*csv'
    for file in files_list:
        basename = os.path.basename(file)        
        if fnmatch.fnmatch(basename, re_patern):
            print(file)
#     remove_files('pchenni-airflow-poc', 's3_emr/in', 'csv')
