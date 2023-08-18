#!/usr/bin/python

'''
Summary:
'''

from parse_kwargs import ParseKwargs

import argparse
import os
import sys
import shutil
import logging
import tomllib

# for DockerOperator in Airflow
logging.basicConfig(stream=sys.stdout, level=logging.INFO)


SETTINGS_TOML = '''
[settings]
mssql_credentials_name = "mssql"
mssql_table_name = "test"
local_export_folder = "test_folder"
file_name = "test_file_name.csv"
cloud_storage_folder = "test_folder_name"
cloud_storage_bucket = "test_bucket_name"
bigquery_table_name = "test.table.name"
'''

SETTINGS = tomllib.loads(SETTINGS_TOML)

# SETTINGS['settings']['file_name']


from enum import Enum
# class Settings(Enum):


def import_arguments() -> dict:
    parser = argparse.ArgumentParser(description='Collecting arguments...')
    parser.add_argument('-k', '--kwargs', nargs='*', action=ParseKwargs)
    return parser.parse_args().kwargs


class Settings:
    '''
    Note: enumeration members must be constants. If something is missing it will throw an error on the missing item.
    '''
    parameters = import_arguments()

    MSSQL_CREDENTIALS_NAME = parameters['mssql_credentials_name']
    MSSQL_TABLE_NAME = parameters['mssql_table_name']
    LOCAL_EXPORT_FOLDER = parameters['local_export_folder']
    FILE_NAME = parameters['file_name']
    CLOUD_STORAGE_FOLDER = parameters['cloud_storage_folder']
    CLOUD_STORAGE_BUCKET = parameters['cloud_storage_bucket']
    BIGQUERY_TABLE_NAME = parameters['bigquery_table_name']



def initial_setup() -> dict:

    list_of_expected_kwargs = [
        'mssql_credentials_name',
        'mssql_table_name', 
        'local_export_folder',
        'file_name',
        'cloud_storage_folder',
        'cloud_storage_bucket',
        'bigquery_table_name'
    ]

    parser = argparse.ArgumentParser(description='Collecting arguments...')
    parser.add_argument('-k', '--kwargs', nargs='*', action=ParseKwargs)
    parameters = parser.parse_args().kwargs
    logging.info(f'Arguments parsed are: {parameters}')

    # Ensure we have everything
    for _, key in enumerate(list_of_expected_kwargs):
        assert isinstance(parameters[key], str), f'Parameter {key} missing!'
        logging.info(f'Quality check passed for {key}')

    return parameters


def main():

    # parameters = initial_setup()

    # print(parameters)

    print(Settings.MSSQL_CREDENTIALS_NAME)




if __name__ == '__main__':
    main()
