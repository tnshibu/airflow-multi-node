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

conn_id = "local_file_system1"
extra=json.dumps({"path":"/usr/local/airflow/dags/request"})

conn = Connection(conn_id=conn_id,conn_type="fs",extra=extra) #create a connection object
session = settings.Session()
conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

if str(conn_name) != str(conn_id):
    session.add(conn)
    session.commit()



args = {
    'owner' : 'avinash',
    'start_date': days_ago(1)
}

dag = DAG(dag_id='file-sensing-local',default_args=args,schedule_interval=None)


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

with dag:
    sensing_task = FileSensor(
        task_id='file-from-local',
        filepath='',
        fs_conn_id=conn_id,
        poke_interval=10        
    )

    read_file_content_task = PythonOperator(
        task_id='read_file_content_task_local_id',
        python_callable=print_file_content,
        provide_context=True,
        retries=10,
        retry_delay=timedelta(seconds=1)
    )

sensing_task >> read_file_content_task