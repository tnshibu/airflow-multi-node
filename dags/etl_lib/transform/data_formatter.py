import pandas as pd
import argparse
import yaml
import sys
import logging
import traceback
import detect_delimiter
import os

# Python magic have different libraries for OS versions
if os.name == 'nt':
    from winmagic import magic
else:
    import magic

"""
data_formatter :
This module will be used to convert data representation format i.e .xlsx, .csv, .xml, etc ... to PIPE_DELIMITED 
flat data representation per the acceptance criteria. 
"""

"""
Start of data_formatter required modules
"""

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(filename)s - %(levelname)s - %(message)s'))
log.addHandler(handler)

app_path = os.path.abspath(os.path.dirname(__file__))


def load_config():
    with open(app_path + '/' + 'data_formatter.yml') as f:
        app_config = yaml.safe_load(f.read())
    log.info('Initializing application config')
    return app_config


def parse_input_params(input_params_dict: dict):
    parser = argparse.ArgumentParser(description='Convert data representation format',
                                     formatter_class=argparse.RawTextHelpFormatter)
    for key, val in input_params_dict.items():
        parser.add_argument('--' + key,
                            action='store',
                            dest=key,
                            required=val['required'],
                            help=val['usage'])

    return parser


# def generate_output_files(data_file_dict: dict):
#     for key, val in data_file_dict.items():
#         try:
#             pd.DataFrame(val['dataframe']).to_csv(val['output_file_path'], sep=config['output_file_delimiter'])
#         except Exception as ex:
#             log.info(ex)
#             sys.exit(1)


def get_file_delimiter(arg_file):
    with open(arg_file, 'rb') as file:
        first_line = str(file.readline().strip())
    delimiter = str(detect_delimiter.detect(first_line))
    log.info('Auto-detection of delimiter for text-type file : [{0}]'.format(delimiter))

    return delimiter


def parse_input_file(arg_file):
    file_format = magic.Magic(mime=True)
    file_type = file_format.from_file(arg_file)
    log.info('Auto-detection of input file type: {0}'.format(magic.from_file(arg_file)))

    if 'text/plain' in file_type:
        log.info('Reader Mode: Text / Flat file Reader')
        delimiter = get_file_delimiter(arg_file)
        df = pd.read_csv(arg_file, sep=delimiter, dtype=str)
    elif 'spreadsheetml' in file_type:
        log.info('Reader Mode: Excel Reader')
        df = pd.read_excel(arg_file, dtype=str)

    return df.fillna('')


def generate_output_file(arg_outpath, arg_df: pd.DataFrame, arg_delim):
    arg_df.to_csv(arg_outpath, index=False, sep=arg_delim)
    log.info('Output generated at : {0}'.format(arg_outpath))


def process(arg_config):
    log.info(arg_config['input_params']['input_file_path'])
    data_df = parse_input_file(arg_config['input_params']['input_file_path'])
    generate_output_file(arg_config['input_params']['output_file_path'], data_df,
                         arg_config['output_file_delimiter'])


"""
Application main block starts
"""

if __name__ == '__main__':
    EXIT_CODE = 0
    try:
        config = load_config()
        input_params = parse_input_params(config['input_params']).parse_args()
        log.info(input_params.__dict__)
        config['input_params'] = input_params.__dict__
        log.info('Starting the process for data formatting')
        process(config)
    except Exception as ex:
        log.error(ex)
        log.error(traceback.format_exc())
        EXIT_CODE = 1
    finally:
        sys.exit(EXIT_CODE)
