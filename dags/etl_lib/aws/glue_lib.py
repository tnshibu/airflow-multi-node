import os
import logging
import time
from datetime import datetime
from pprint import pprint

import boto3
import yaml

def get_glue_client():
    # return boto3.client('glue')
    return boto3.client(service_name='glue', region_name='us-east-1',
                        endpoint_url='https://glue.us-east-1.amazonaws.com')

glue_client = get_glue_client()

def get_crawler(crawler_name):
    return glue_client.get_crawler(Name=crawler_name).get('Crawler')

def get_all_crawlers():
    return glue_client.get_crawlers().get('Crawlers')

def get_all_crawler_names():
    all_crawlers_list = get_all_crawlers()
    ret_list = []
    for crawler in all_crawlers_list:
        ret_list.append(crawler['Name'])
    return ret_list

def get_crawler_s3_paths(crawler_name):
    ret_list = []
    crawler = get_crawler(crawler_name)
    cr_tgts = crawler['Targets']
    for tgt_type, tgt_props in cr_tgts.items():
        if tgt_type == 'S3Targets':
            for paths in cr_tgts[tgt_type]:
                print(paths.get('Path', None))
                ret_list.append(paths.get('Path', None))
    return ret_list


def get_all_crawlers_s3_path():
    all_crawlers_list = get_all_crawlers()
    ret_list = []
    for crawler in all_crawlers_list:
        cr_tgts = crawler['Targets']
        for tgt_type, tgt_props in cr_tgts.items():
            if tgt_type == 'S3Targets':
                for paths in cr_tgts[tgt_type]:
                    print(paths.get('Path', None))
                    ret_list.append(paths.get('Path', None))
    return ret_list


def update_crawler_target(crawler_name, target_path, target_exclusions=[], target_type='S3'):
    logging.info(f'Updating {crawler_name} with {target_path}')
    crawler_obj = get_crawler(crawler_name)
    if crawler_obj:
        if 'RUNNING' == get_crawler_status(crawler_name):
            logging.info(
                'Crawler is running , stopping the crawler before update!')
            stop_crawler(crawler_name)
        # exisiting_paths = get_crawler_s3_paths(crawler_name)
        # exisiting_paths.append(target_path)
        new_path = {'Path': target_path, 'Exclusions': target_exclusions}
        crawler_obj['Targets']['S3Targets'].append(new_path)
        update_crawler(crawler_obj)


def update_crawler(crawler_obj):
    keys_to_remove = ['State', 'CrawlElapsedTime',
                      'CreationTime', 'LastUpdated', 'LastCrawl', 'Version', 'Exclusions']
    new_crawler_obj = {k: v for k,
                       v in crawler_obj.items() if k not in keys_to_remove}
    glue_client.update_crawler(**new_crawler_obj)


def stop_crawler(crawler_name):
    response = glue_client.stop_crawler(Name=crawler_name)
    while 'READY' == get_crawler_status(crawler_name):
        logging.info('Crawler is stopping...')
        time.sleep(5)
    return response


def start_crawler(crawler_name):
    try:
        glue_client.start_crawler(Name=crawler_name)
    except Exception as e:
        logging.info(e)
    return crawler_name


def get_crawler_status(crawler_name):
    crawler_obj = get_crawler(crawler_name)
    print(crawler_obj['State'])
    return crawler_obj['State']


def wait_for_crawler(crawler_name):
    MAX_RETRIES=20
    retry_count = 0
    while 'READY' != get_crawler_status(crawler_name):
        logging.info(f'Waiting for {crawler_name} to complete..')
        retry_count += 1
        if retry_count == MAX_RETRIES:
            logging.info(f'Crawler {crawler_name} has not started and has reached timeout.')
            break
        time.sleep(30)
    logging.info(f'Crawler {crawler_name} has ran succesfully..')


def create_crawler(crawler_name, **kwargs):
    crawler_cfg_dir = '/usr/local/airflow/config/crawlers'
    crawler_cfg_file = os.path.join(crawler_cfg_dir, f'{crawler_name}.yaml')
    if os.path.exists(crawler_cfg_file):
        with open(crawler_cfg_file, mode='r') as cfg_file:
            crawler_defn = yaml.load(cfg_file)
            response = glue_client.create_crawler(**crawler_defn)
        print(response)


# Glue Job Methods
def get_glue_job_detail(job_name):
    return glue_client.get_job(JobName=job_name)

def get_all_glue_jobs():
    return glue_client.get_jobs()

def get_all_glue_job_names():
    jobs_list = get_all_glue_jobs()['Jobs']
    ret_list = []
    for job in jobs_list:
        ret_list.append(job['Name'])
    print(ret_list)
    return ret_list

def start_glue_job(gj_name):
    try:
        job_info = glue_client.start_job_run(JobName=gj_name)
    except Exception as e:
        logging.info(e)
    return job_info


def get_glue_job_run_status(gj_name, job_info):
    job_run_info = glue_client.get_job_run(JobName=gj_name, RunId=job_info['JobRunId'])
    print(job_run_info['JobRun']['JobRunState'])
    return job_run_info['JobRun']['JobRunState']


def wait_for_glue_job_run(gj_name, job_info):
    MAX_RETRIES=20
    retry_count = 0
    while 'RUNNING' == get_glue_job_run_status(gj_name, job_info):
        logging.info(f'Waiting for {gj_name} to complete..')
        retry_count += 1
        if retry_count == MAX_RETRIES:
            logging.info(f'Glue Job {gj_name} has not completed and has reached timeout.')
            break
        time.sleep(30)
    logging.info(f'Glue Job {gj_name} has completed..')


def create_glue_job(gj_name, **kwargs):
    gj_cfg_dir = '/usr/local/airflow/config/glue_jobs'
    gj_cfg_file = os.path.join(gj_cfg_dir, f'{gj_name}.yaml')
    if os.path.exists(gj_cfg_file):
        with open(gj_cfg_file, mode='r') as cfg_file:
            glue_job_defn = yaml.load(cfg_file)
            response = glue_client.create_job(**glue_job_defn)
        print(response)
