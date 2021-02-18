import airflow
import logging
import os
import shutil
import json

from pathlib import Path
from airflow import DAG
from airflow.models import DAG
from airflow.utils.dates import days_ago,timedelta
from airflow.contrib.hooks.fs_hook import FSHook
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow import settings
from airflow.models import Connection
from airflow.utils.email import send_email

SLACK_CONN_ID = 'slack-notification'
SLACK_CONN_ID = 'slack-notification'
TO_MAIL = "avinash.kachhwaha@oracle.com"

conn_id = "local_file_system1"
extra=json.dumps({"path":"/usr/local/airflow/dags/request"})

conn = Connection(conn_id=conn_id,conn_type="fs",extra=extra) #create a connection object
session = settings.Session()
conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

if str(conn_name) != str(conn_id):
    session.add(conn)
    session.commit()


def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack-notification',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    return failed_alert.execute(context=context)

def callable_func(context):
    print(context)
    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id
    log_url = context.get('task_instance').log_url
    log_filepath = context.get('task_instance').log_filepath
    exec_date = context.get('execution_date')

    email_title = """
    Airflow Alert-- Airflow Task {task_id} is Success
    """.format(task_id = task_id)

    email_body = """
    <table class="reportWrapperTable" cellspacing="4" cellpadding="2" width="100%" rules="rows" style="border-collapse:collapse;color:#1f2240;background-color:#ffffff">
    <caption style="background-color:#ffffff;color:#1f2240;margin-bottom:.5em;font-size:18pt;width:100%;border:0">Airflow Alert</caption>
    <thead style="100%;color:#ffffff;background-color:#1f2240;font-weight:bold">
    <tr>
    <th scope="col" style="background-color:#1f2240">Name</th>
    <th scope="col" style="background-color:#1f2240">Status</th>
    </tr>
    </thead>
    <tbody>

    <tr>
    <td>The Task: </td>
    <td>{task_id}</td>
    </tr>
    
    <tr>
    <td>The Dag: </td>
    <td>{dag_id}</td>
    </tr>

    <tr>
    <td>Execution Time: </td>
    <td>{exec_date}</td>
    </tr>

    <tr>
    <td>Status: </td>
    <td> <span style="font-size: 20px; color: #008000;">
    Success.
    </span>
    </td>
    </tr>

    <tr>
    <td>Log Url: </td>
    <td><a href="{log_url}">Link</a></td>
    </tr>
    </tbody></table>
    """.format(task_id = task_id, dag_id = dag_id,log_url=log_url,exec_date=exec_date)
    to = TO_MAIL
    send_email(to, email_title, email_body)

args = {
    'owner' : 'avinash',
    'start_date': days_ago(1),
    'email': ['avinash.kachhwaha@oracle.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'provide_context': True,
    'on_success_callback': callable_func,
    'on_failure_callback' : task_fail_slack_alert,
}

dag = DAG(dag_id='file-sensing-local',
default_args=args,
schedule_interval=None)


def print_file_content(**context):
    foldername = "/processed"
    hook = FSHook(conn_id)
    parentPath = str(Path(hook.get_path()).parent)
    print(parentPath)
    if not os.path.exists(parentPath + foldername):
	    os.makedirs(parentPath +  foldername)

    for file in os.listdir(hook.get_path()):
        if file.endswith(".txt"):
            with open(hook.get_path()+"/"+file, 'r') as fp:
                print(fp.read())
                shutil.move(hook.get_path()  +"/" + file  , parentPath + foldername + "/" + file)
        else:
            os.remove(os.path.join(hook.get_path(), file))


sensing_task = FileSensor(
        task_id='file-from-local',
        filepath='',
        fs_conn_id=conn_id,
        poke_interval=10,
        dag=dag     
)

read_file_content_task = PythonOperator(
        task_id='read_file_content_task_local_id',
        python_callable=print_file_content,
        provide_context=True,
        retries=10,
        retry_delay=timedelta(seconds=1),
        dag=dag
)

sensing_task >> read_file_content_task