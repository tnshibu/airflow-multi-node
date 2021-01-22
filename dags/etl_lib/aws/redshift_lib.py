import logging

import boto3
from airflow.contrib.hooks.redshift_hook import RedshiftHook
from airflow.hooks.postgres_hook import PostgresHook

def get_rs_client():
    return boto3.client('redshift')

def get_rs_hook(rs_conn_id):
    return RedshiftHook(aws_conn_id=rs_conn_id)

def get_pg_hook(pg_conn_id):
    return PostgresHook(postgres_conn_id=pg_conn_id, autocommit=True)

def cluster_status(cluster_identifier):
    """
    Return status of a cluster

    :param cluster_identifier: unique identifier of a cluster
    :type cluster_identifier: str
    """
    conn = get_rs_client()
    try:
        response = conn.describe_clusters(
            ClusterIdentifier=cluster_identifier)['Clusters']
        return response[0]['ClusterStatus'] if response else None
    except conn.exceptions.ClusterNotFoundFault:
        return 'cluster_not_found'

def check_rs_table_exists(conn_id, tbl_name,**kwargs):
    rs_hook = get_pg_hook(conn_id)
    result = rs_hook.get_first(f"""select true where EXISTS (SELECT *
                                FROM INFORMATION_SCHEMA.TABLES
                                WHERE TABLE_NAME = '{tbl_name.lower()}'); """)
    print(result)
    if result and result[0]:
        logging.info(
            f'Table {tbl_name} already exists.. Good to proceed with load..')
        return True
    else:
        logging.info(
            f'Table {tbl_name} does not exists.. Please proceed after table creation..')
        return False

if __name__ == "__main__":
    check_rs_table_exists('aws_redshift', 'columns')
