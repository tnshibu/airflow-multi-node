from airflow import DAG
from datetime import datetime, timedelta
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.operators.python_operator import PythonOperator
# from etl_lib.transform import data_formatter
import etl_lib.aws.s3_lib as s3_helper

default_args = {
    'owner': 'ofss',
    'email': ['prabakaran.chenni@oracle.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retires': 5,
    'start_date': datetime.now() - timedelta(days=1),
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
    'depends_on_past': False
}
# Global Variables
S3_CONNECTION_ID = 's3_airflow'
S3_BUCKET_NAME = 'ofss-compute-aws'
INPUT_DIR = 'market_data/in'
# CURR_DATE_YYYYMMDD = datetime.now().strftime('%Y%m%d')
CURR_DATE_YYYYMMDD = '20200508'


def trigger_data_formatter(**context):
    files_list = s3_helper.get_file_list(
        S3_BUCKET_NAME, f'{INPUT_DIR}/ticker*data*{CURR_DATE_YYYYMMDD}.csv')
    if files_list:
        for in_file in files_list:
            print(in_file)


with DAG('aws_dp_poc', default_args=default_args, schedule_interval=None) as dag:
    in_file_sensor = S3KeySensor(
        task_id='in_file_sensor',
        bucket_key=f'{INPUT_DIR}/ticker*data*{CURR_DATE_YYYYMMDD}.csv',
        wildcard_match=True,
        bucket_name=f'{S3_BUCKET_NAME}',
        aws_conn_id=S3_CONNECTION_ID,
        poke_interval=60 * 2,  # (seconds); checking file every 2 minutes
        timeout=60 * 4,  # timeout in 4 minutes
    )

    trigger_transform = PythonOperator(
        task_id='trigger_transform',
        python_callable=trigger_data_formatter
    )

    in_file_sensor >> trigger_transform
