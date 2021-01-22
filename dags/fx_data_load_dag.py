import json
import logging
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import (BranchPythonOperator,
                                               PythonOperator)
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from airflow.sensors.http_sensor import HttpSensor
from airflow.sensors.s3_key_sensor import S3KeySensor
from pandas.io.json import json_normalize

import etl_lib.aws.redshift_lib as rs_lib
from etl_lib.airflow.operators.redshift_op import RedshiftLoadTableOperator

default_args = {
    'owner': 'prabakaran',
    # 'start_date': datetime.now() - timedelta(days=1),
    'start_date': datetime(2020, 6, 18),
    'depends_on_past': False,
    'email_on_retry': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    # 'catchup_by_default': False
}

# Global Variables
S3_CONNECTION_ID = 's3_airflow'
S3_BUCKET_NAME = 'ofss-compute-aws'
STATIC_DIR = 'fx_data/static'
OUTPUT_DIR = 'fx_data/out'
REDSHIFT_CONN_ID = 'aws_redshift'
REDSHIFT_CLUSTER_ID = 'vnc-rs-cluster1'
RS_FX_TABLE_NAME = 'fx_rates'


def download_fx_rates(arg_date, file_date, **context):
    fx_file = f's3://{S3_BUCKET_NAME}/{STATIC_DIR}/fx_currencies.csv'
    currency_list = pd.read_csv(fx_file, header=None)[0].to_list()
    api_data_list = []
    for currency in currency_list:
        api_url = f'https://api.exchangeratesapi.io/{arg_date}?base=' + currency.strip()
        logging.info(f'URL:::{api_url}')
        api_data = requests.get(api_url).json()
        api_data_list.append(api_data)
    logging.info(f'FX_DATA:::{api_data_list}')
    api_df = json_normalize(api_data_list)
    api_df.columns = api_df.columns.map(lambda x: x.split(".")[-1])
    api_df = api_df.melt(id_vars=["base", "date"],
                         var_name="TARGET_CURRENCY",
                         value_name="FX_RATE_VALUE")
    api_df.rename(columns={'base': 'BASE_CURRENCY',
                           'date': 'ASOF_DATE'}, inplace=True)
    api_df.sort_values(['BASE_CURRENCY', 'ASOF_DATE'], inplace=True)
    api_df['LOAD_TIMESTAMP'] = pd.Timestamp.now()
    s3_out_file = f's3://{S3_BUCKET_NAME}/{OUTPUT_DIR}/fx_rates_{file_date}.csv'
    api_df.to_csv(s3_out_file, index=False)


def check_db_status():
    # if rs_lib.check_rs_table_exists(REDSHIFT_CONN_ID, RS_FX_TABLE_NAME):
    #         return 'load_fx_data'
    # rs_hook = rs_lib.get_rs_hook(REDSHIFT_CONN_ID)
    rs_status = rs_lib.cluster_status(REDSHIFT_CLUSTER_ID)
    logging.info(f'Cluster Status::::{rs_status}')
    if rs_status == 'available':
        logging.info(
            f'{REDSHIFT_CLUSTER_ID} is available. Proceeding with table check..')
        if rs_lib.check_rs_table_exists(REDSHIFT_CONN_ID, RS_FX_TABLE_NAME):
            return 'load_fx_data'
    return 'trigger_download_email'


with DAG(dag_id='fx_data_load', default_args=default_args, schedule_interval='@daily', catchup=True) as dag:
    check_forex_api = HttpSensor(
        task_id='check_forex_api',
        endpoint='latest',
        http_conn_id='forex_api',
        method='GET',
        response_check=lambda response: 'rates' in response.text,
        poke_interval=5,
        timeout=20
    )
    check_currencies_file = S3KeySensor(
        task_id='check_currencies_file',
        bucket_key=f'{STATIC_DIR}/fx_currencies.csv',
        wildcard_match=True,
        bucket_name=f'{S3_BUCKET_NAME}',
        aws_conn_id=S3_CONNECTION_ID,
        poke_interval=60,  # (seconds); checking file every 2 minutes
        timeout=60 * 2,  # timeout in 2 minutes
    )

    download_fx_data = PythonOperator(
        task_id='download_fx_data',
        python_callable=download_fx_rates,
        op_args={
            '{{ ds }}',
            '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y%m%d") }}'
        },
        provide_context=True
    )

    check_db_status = BranchPythonOperator(
        task_id='check_db_status',
        python_callable=check_db_status
    )

    # load_fx_data = S3ToRedshiftTransfer(
    #     task_id='load_fx_data',
    #     schema='airflow_poc',
    #     redshift_conn_id=REDSHIFT_CONN_ID,
    #     table=RS_FX_TABLE_NAME,
    #     aws_conn_id=S3_CONNECTION_ID,
    #     s3_bucket=S3_BUCKET_NAME,
    #     s3_key=f'{OUTPUT_DIR}/fx_rates_{globals()["execution_date"]}.csv',
    #     copy_options=''
    # )

    load_fx_data = RedshiftLoadTableOperator(
        task_id='load_fx_data',
        conn_id=REDSHIFT_CONN_ID,
        bucket_name=S3_BUCKET_NAME,
        csv_file=f'{OUTPUT_DIR}/' +
        'fx_rates_{{ macros.ds_format(ds, "%Y-%m-%d", "%Y%m%d") }}.csv',
        table_name=RS_FX_TABLE_NAME,
    )

    trigger_download_email = EmailOperator(
        task_id='trigger_download_email',
        subject='{{ macros.ds_format(ds, "%Y-%m-%d", "%Y%m%d") }} - FX Rates downloaded to S3!!!',
        # to=['prabakaran.chenni@oracle.com', 'bhushan.oulkar@oracle.com', 'ananda.shivamurthy@oracle.com',
        #     'haravijaya.makuru@oracle.com', 'satyaravikiran.godavarti@oracle.com', 'satyabrat.sahoo@oracle.com'],
        to=['prabakaran.chenni@oracle.com'],
        html_content="""
        Hi All,<br/>
        <h3>Forex data downloaded and saved to S3 Bucket for {{ macros.ds_format(ds, "%Y-%m-%d", "%Y%m%d") }} </h3><br/>
        <p>Redshift DB was unavailable during the load. Please backfill using the downloaded files..</p>
        
        Thanks & Regards<br/>
        Prabakaran C
        """
    )

    trigger_load_email = EmailOperator(
        task_id='trigger_load_email',
        subject='{{ macros.ds_format(ds, "%Y-%m-%d", "%Y%m%d") }} - FX Rates Loaded to Redshift!!!',
        # to=['prabakaran.chenni@oracle.com', 'bhushan.oulkar@oracle.com', 'ananda.shivamurthy@oracle.com',
        #     'haravijaya.makuru@oracle.com', 'satyaravikiran.godavarti@oracle.com', 'satyabrat.sahoo@oracle.com'],
        to=['prabakaran.chenni@oracle.com'],
        html_content="""
        Hi All,<br/>
        <h3>Forex data downloaded and loaded succesfully to Redshift table for {{ macros.ds_format(ds, "%Y-%m-%d", "%Y%m%d") }} </h3><br/>
        
        Thanks & Regards<br/>
        Prabakaran C
        """
    )

    check_forex_api >> check_currencies_file >> download_fx_data >> check_db_status
    check_db_status >> load_fx_data >> trigger_load_email
    check_db_status >> trigger_download_email
