import json
import datetime
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator



from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.sensors import HttpSensor
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow import settings
from airflow.models import Connection

BASE_URL='http://192.168.1.7:8181/employee'    
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['avinash.kachhwaha@oracle.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'provide_context': True

}

dag = DAG(
    'http_operator_rest_curl',
    default_args=default_args,
    tags=['example'],
    start_date=datetime.datetime.now() - datetime.timedelta(days=1)
    # schedule_interval='*/1 * * * *',
)

dag.doc_md = __doc__
  

health_check_task = BashOperator(
    task_id='status-check',
    bash_command='curl -X GET {BASE_URL}/'.format(BASE_URL = BASE_URL),
    dag=dag
)

get_all_employee_task = BashOperator(
    task_id='get-all-employee',
    bash_command='curl -X GET {BASE_URL}/get-all'.format(BASE_URL = BASE_URL),
    xcom_push=True,
    dag=dag
)

get_employee_by_id_task = BashOperator(
    task_id='get-employee-by-id',
    bash_command='curl -X GET {BASE_URL}/get-by-id/3/employee'.format(BASE_URL = BASE_URL),
    xcom_push=True,
    dag=dag
)

jsondata='{"name" : "Vikram3"}'
save_employee_task = BashOperator(
    task_id='save-employee',
    bash_command='curl -X POST  --data \'{jsondata}\' -H "Content-Type: application/json"  {BASE_URL}/save'.format(jsondata=jsondata, BASE_URL = BASE_URL),
    xcom_push=True,
    dag=dag
)

health_check_task >> save_employee_task >> get_all_employee_task
health_check_task >> save_employee_task >> get_employee_by_id_task
