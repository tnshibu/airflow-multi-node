import airflow
import logging
import os

from airflow import DAG
from airflow.models import DAG
from airflow.utils.dates import days_ago,timedelta

from airflow.contrib.hooks.fs_hook import FSHook

from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator

args = {
    'owner' : 'avinash',
    'start_date': days_ago(1)
}

dag = DAG(dag_id='my_sample_dag',default_args=args,schedule_interval=None)

def print_file_content(**context):
    hook = FSHook('my_file_system')
    path = os.path.join(hook.get_path(), 'test.txt')
    with open(path, 'r') as fp:
        print(fp.read())
    os.remove(path)


with dag:
    sensing_task = FileSensor(
        task_id='sensing_task',
        filepath='test.txt',
        fs_conn_id='my_file_system',
        poke_interval=10        
    )

    read_file_content_task = PythonOperator(
        task_id='read_file_content_task_id',
        python_callable=print_file_content,
        provide_context=True,
        retries=10,
        retry_delay=timedelta(seconds=1)
    )

sensing_task >> read_file_content_task