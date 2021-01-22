import glob
import logging
import os
import re

import pandas as pd


def convert_header_case(arg_hdr_list):
    ret_col_list = []
    for hdr in arg_hdr_list:
        hdr_name = str.strip(hdr)
        hdr_name = re.sub(r'\W+', '_', hdr_name)
        # If Column name is already in upper case just add it
        if hdr_name.isupper():
            ret_col_list.append(hdr_name)
        # If Column name is in lower case conver to upper and add it
        elif hdr_name.isupper():
            ret_col_list.append(hdr_name.upper())
        # If Column name is in camelcase convert to upper and add it
        else:
            formatted_hdr = ''
            for i in range(len(hdr_name)):
                if i == 0:
                    formatted_hdr = formatted_hdr + hdr_name[i].upper()
                    continue
                if hdr_name[i].isupper() and i != len(hdr_name)-1:
                    formatted_hdr = formatted_hdr + '_' + hdr_name[i]
                elif hdr_name[i] != '_':
                    formatted_hdr = formatted_hdr + hdr_name[i].upper()
            ret_col_list.append(formatted_hdr)
    return ret_col_list


def convert_bool_to_str(arg_df):
    mask = arg_df.applymap(type) != bool
    d = {True: 'TRUE', False: 'FALSE'}
    return arg_df.where(mask, arg_df.replace(d))


def gen_table_schema(arg_csv_file, arg_tbl_name, arg_conn=None, arg_bucket_name=None):
    if arg_bucket_name:
        csv_df = pd.read_csv(f's3://{arg_bucket_name}/{arg_csv_file}')
    else:
        csv_df = pd.read_csv(arg_csv_file)
    # Converting Boolean to String as Pandas converts booleans to 1s and 0s
    csv_df = convert_bool_to_str(csv_df)
    conv_header_cols = convert_header_case(csv_df.columns.tolist())
    # print(conv_header_cols)
    csv_df.columns = conv_header_cols
    ret_sql = ''
    # If DB connection is passed, DDL will be created with supported datatypes of the DB
    if arg_conn:
        ret_sql = pd.io.sql.get_schema(csv_df, arg_tbl_name, con=arg_conn)
        arg_conn.close()
    else:
        ret_sql = pd.io.sql.get_schema(csv_df, arg_tbl_name)
    ret_sql = ret_sql.replace('"', '')
    if arg_bucket_name:
        ret_sql = re.sub(r'\bTEXT\b', 'VARCHAR(MAX)', ret_sql)
        # ret_sql = ret_sql.replace('TEXT', 'VARCHAR(MAX)')
    logging.info(f'DDL generated for the CSV:::{arg_csv_file}')
    logging.info(f'{ret_sql}')
    # print(ret_sql)
    return ret_sql


def get_tbl_name(arg_file):
    file_name_wo_ext, file_ext = os.path.splitext(os.path.basename(arg_file))
    return file_name_wo_ext.replace(' ', '_').upper()


def get_files_from_dir(arg_dir, arg_file_pattern):
    return glob.glob(os.path.join(arg_dir, arg_file_pattern))


# if __name__ == "__main__":
#     # gen_table_schema('/usr/local/airflow/data/electronics_price_data.csv',
#     # 'electronics')
#     gen_table_schema('electronics_price_data.csv',
#                      'electronics_price_data', None, 'pchenni-airflow-poc')
