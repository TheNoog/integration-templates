#!/usr/bin/python

'''
Summary:
'''

from parse_kwargs import ParseKwargs

import argparse
import sys
import logging


# for DockerOperator in Airflow
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

class Settings:
    '''
    Note: enumeration members must be constants. If something is missing it will throw an error on the missing item.
    '''
    parser = argparse.ArgumentParser(description='Collecting arguments...')
    parser.add_argument('-k', '--kwargs', nargs='*', action=ParseKwargs)
    args = parser.parse_args().kwargs

    MSSQL_CREDENTIALS_NAME = args['mssql_credentials_name']
    MSSQL_TABLE_NAME = args['mssql_table_name']
    LOCAL_EXPORT_FOLDER = args['local_export_folder']
    FILE_NAME = args['file_name']
    CLOUD_STORAGE_FOLDER = args['cloud_storage_folder']
    CLOUD_STORAGE_BUCKET = args['cloud_storage_bucket']
    BIGQUERY_TABLE_NAME = args['bigquery_table_name']



def main():


    print(Settings.MSSQL_CREDENTIALS_NAME)
    print(Settings.BIGQUERY_TABLE_NAME)




if __name__ == '__main__':
    main()
