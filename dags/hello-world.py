import datetime
from datetime import timedelta

import airflow
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def hello_world_1():
    logging.info("Hello World 1")
    logging.info(f"6 -2 = {6-2}")

def hello_world_2():
    logging.info("Hello World 2")
    logging.info(f"9 -2 = {9-2}")


hello_dag = DAG(
        "lesson1.excercise1",
        start_date=datetime.datetime.now(),
        schedule_interval='@daily',
        description='A simple hello world DAG'

)

t1 = PythonOperator(
        task_id="hello_world_task_1",
        python_callable=hello_world_1,
        dag=hello_dag
)


t2 = PythonOperator(
        task_id="hello_world_task_2",
        python_callable=hello_world_2,
        dag=hello_dag
)

t1 >> t2


