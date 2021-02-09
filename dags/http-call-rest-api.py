import json
import datetime
from datetime import timedelta


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

conn_id = "rest-connection1"
host="http://3.129.42.101:8181/employee"

conn = Connection(conn_id=conn_id,conn_type="http",host=host,) #create a connection object
session = settings.Session()
conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

if str(conn_name):
    session.delete(conn_name)    
    session.add(conn)
    session.commit()  
    session.close()


    
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
    'http_operator_rest_call',
    default_args=default_args,
    tags=['example'],
    start_date=datetime.datetime.now() - datetime.timedelta(days=1)
        # schedule_interval='*/1 * * * *',

    )

dag.doc_md = __doc__
  

from airflow.utils.email import send_email

# transform the json here and save the content to a file
def save_emp_json(**kwargs):
    ti = kwargs['ti']
    employee = ti.xcom_pull(key=None, task_ids='save_employee')
    import_body= json.loads(employee)
    print(import_body)
    id =   import_body["id"]
    print(id)
    empid = id
    Variable.set("id", empid)

 # transform the json here and save the content to a file
task_save_employee = SimpleHttpOperator(
    task_id='save_employee',
    http_conn_id=conn_id,
    endpoint='/save',
    method="POST",
    data=json.dumps({"name": "avinash singh"}),
    headers={"Content-Type": "application/json"},
    # response_check=lambda response: response.json()['json']['name'] == "avinash singh",
    xcom_push=True,
    dag=dag,
)


task_get_all_employee = SimpleHttpOperator(
    task_id='get_all_employee',
    http_conn_id=conn_id,
    endpoint='/get-all',
    method="GET",
    headers={"Content-Type": "application/json"},
    response_filter=lambda response: response.json(),
    xcom_push=True,
    dag=dag,
)


task_get_byid_employee = SimpleHttpOperator(
    task_id='get-by-id-employee',
    http_conn_id=conn_id,
    endpoint="/get-by-id/{empId}/employee".format(empId = Variable.get("id")),
    method="GET",
    headers={"Content-Type": "application/json"},
    response_filter=lambda response: response.json(),
    xcom_push=True,
    dag=dag,
)


# [END howto_operator_http_task_del_op]
# [START howto_operator_http_http_sensor_check]
task_http_sensor_check = HttpSensor(
    task_id='api_health_check',
    http_conn_id=conn_id,
    endpoint='/',
    request_params={},
    poke_interval=15,
    dag=dag,
)

save_employee = PythonOperator(
    task_id="save_employee_transform", 
    python_callable=save_emp_json,
    provide_context=True
)

task_http_sensor_check >> task_save_employee >> save_employee >> task_get_byid_employee
save_employee >> task_get_all_employee
