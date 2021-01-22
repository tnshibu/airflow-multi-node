import logging

import etl_lib.aws.redshift_lib as rs_lib
import etl_lib.helpers.file_helper as file_helper
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

AWS_IAM_ROLE = 'arn:aws:iam::555778330973:role/vncRedshiftRole'
AWS_REGION = 'us-east-1'


class RedshiftCreateTableOperator(BaseOperator):
    @apply_defaults
    def __init__(self, conn_id, csv_file, table_name, bucket_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._conn_id = conn_id
        self._csv_file = csv_file
        self._table_name = table_name
        self._bucket_name = bucket_name

    def execute(self, context):
        pg_hook = rs_lib.get_pg_hook(self._conn_id)
        create_ddl_sql = file_helper.gen_table_schema(
            self._csv_file, self._table_name, pg_hook.get_conn(), self._bucket_name)
        drop_stmt = f'DROP TABLE IF EXISTS {self._table_name};'
        create_ddl_sql = f'{drop_stmt}\n{create_ddl_sql};'
        pg_hook.run(create_ddl_sql)
        # return super().execute(context)


class RedshiftLoadTableOperator(BaseOperator):
    template_fields = ['_csv_file']
    @apply_defaults
    def __init__(self, conn_id, bucket_name, csv_file, table_name=None, mode='append', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._conn_id = conn_id
        self._csv_file = csv_file
        self._bucket_name = bucket_name
        if table_name:
            self._table_name = table_name
        else:
            self._table_name = file_helper.get_tbl_name(csv_file)
        self._mode = mode

    def execute(self, context):
        pg_hook = rs_lib.get_pg_hook(self._conn_id)
        if self._mode == 'truncate':
            logging.info(
                f'Truncating table {self._table_name} before loading..')
            trunc_sql = f'TRUNCATE TABLE {self._table_name};'
            pg_hook.run(trunc_sql)
        self.load_to_postgres_table(pg_hook)

    def load_to_postgres_table(self, arg_pg_hook):
        # load_sql = f"""copy {self._table_name} from 's3://{self._bucket_name}/{self._csv_file}' 
        #             credentials 'aws_iam_role={AWS_IAM_ROLE}' 
        #             delimiter ',' region '{AWS_REGION}' IGNOREHEADER 1 REMOVEQUOTES;"""
        load_sql = f"""copy {self._table_name} from 's3://{self._bucket_name}/{self._csv_file}' 
                    credentials 'aws_access_key_id=AKIAYCZXYKFOZ5JFZGRO;aws_secret_access_key=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' 
                    delimiter ',' IGNOREHEADER 1 REMOVEQUOTES;"""
        arg_pg_hook.run(load_sql)


class RedshiftProcOperator(BaseOperator):
    @apply_defaults
    def __init__(self, conn_id, sp_name, proc_args=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._conn_id = conn_id
        self._sp_name = sp_name
        self._proc_args = proc_args

    def execute(self, context):
        pg_hook = db_helper.get_pg_hook(self._conn_id)
        proc_sql = f"""CALL {self._sp_name}();"""
        pg_hook.run(proc_sql)
