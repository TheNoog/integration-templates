import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import json
import logging
import numpy as np
import pandas as pd
import pandas.io.sql as sqlio
from io import StringIO

class snowflakeCredentials(object):
    @staticmethod
    def get_credentials(credentials_file_path):
        with open(credentials_file_path) as credentials_file:
            credentials = json.load(credentials_file)
            return snowflakeCredentials(**credentials)

    def __init__(self, account, password, user):
        self.user = user
        self.password = password
        self.account = account


class Snowflake_(object):
    @staticmethod
    def from_config(config):
        snowflake = Snowflake_(config.snowflake_.credentials_file_path)
        return snowflake

    def __init__(self, credentials_file_path):
        self._credentials_file_path = credentials_file_path

    def _engine(self, warehouse=None, database=None, schema=None):
        try:
            credentials = snowflakeCredentials.get_credentials(self._credentials_file_path)
            conn = snowflake.connector.connect(
                    user=credentials.user,
                    password=credentials.password,
                    account=credentials.account
                )
            
            if warehouse != None:
                conn.cursor().execute(f"USE WAREHOUSE {warehouse}")
            
            if database != None:
                conn.cursor().execute(f"USE DATABASE {database}")
            
            if schema != None:
                conn.cursor().execute(f"USE SCHEMA {schema}")

            return conn
        except Exception:
            logging.exception(f"\n\n ERROR: Missing information to create Snowflake engine.")
            raise        


    def execute(self, sql, warehouse, database, schema):
        engine = self._engine(warehouse=warehouse, database=database, schema=schema)
        try:
            engine.cursor().execute(sql)
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue executing code...")
            raise
        finally:
            engine.cursor().close()
            engine.close()
        return f"SQL executed: {sql}."


    def testConnection(self, warehouse, database, schema):
        sql = "SELECT current_version()"
        try:
            self.execute(sql, warehouse, database, schema)
            logging.info(f"Connection SUCCESS! for {warehouse}.{schema}.{database}")
        except Exception:
            logging.exception(f"\n\n testConnection failed!")
            raise


    def createDatabase(self, warehouse, database):
        try:
            self.execute(f"CREATE OR REPLACE DATABASE {database}", warehouse, database=None, schema=None)
            logging.info(f"Database creation SUCCESS! for {warehouse}.{database}")
        except Exception:
            logging.exception(f"\n\n createDatabase failed!")
            raise


    def dropDatabase(self, warehouse, database):
        try:
            self.execute(f"DROP DATABASE {database}", warehouse, database=None, schema=None)
            logging.info(f"Database creation SUCCESS! for {warehouse}.{database}")
        except Exception:
            logging.exception(f"\n\n dropDatabase failed!")
            raise


    def createSchema(self, warehouse, database, schema):
        try:
            self.execute(f"CREATE OR REPLACE SCHEMA {schema}", warehouse, database, schema=None)
            logging.info(f"Schema creation SUCCESS! for {warehouse}.{schema}.{database}")
        except Exception:
            logging.exception(f"\n\n createSchema failed!")
            raise


    def dropSchema(self, warehouse, database, schema):
        try:
            self.execute(f"DROP SCHEMA {schema}", warehouse, database, schema=None)
            logging.info(f"Schema creation SUCCESS! for {warehouse}.{schema}.{database}")
        except Exception:
            logging.exception(f"\n\n dropSchema failed!")
            raise


    def dtype_mapping(self):
        return {'object' : 'TEXT',
            'int64' : 'INT',
            'float64' : 'FLOAT',
            'datetime64' : 'DATETIME',
            'bool' : 'INT',
            'category' : 'TEXT',
            'timedelta[ns]' : 'DATETIME'}


    def createTableFromDataframe(self, dataframe, table_name, warehouse, database, schema):

        snowflake_sql = f"CREATE OR REPLACE TABLE {table_name}("

        for i in range(0, len(list(dataframe.columns))):
            attr = list(dataframe.columns)[i]
            dmap = self.dtype_mapping()
            d_type = list(dataframe.dtypes)[i]
            snowflake_sql = snowflake_sql + f"{attr} {dmap[str(d_type)]}, "

        snowflake_sql = snowflake_sql[:-2] + ")"  

        logging.info(snowflake_sql)

        try: 
            self.execute(snowflake_sql, warehouse, database, schema)
            logging.info(f"Table created. {warehouse}.{schema}.{database}.{table_name}")
        except Exception:
            logging.exception(f"\n\n ERROR: Couldn't create the table.")
            raise


    def uploadFromDataframeToTable(self, dataframe, table_name, warehouse, database, schema):

        engine = self._engine(warehouse=warehouse, database=database, schema=schema)

        dataframe.columns = dataframe.columns.str.upper()

        try: 
            logging.info(f"Starting data upload...")
            write_pandas(engine, dataframe, table_name)
            logging.info(f"Data upload SUCCESS. {warehouse}.{schema}.{database}.{table_name}")
        except Exception:
            logging.exception(f"\n\n ERROR: Couldn't upload the data to table. {warehouse}.{schema}.{database}.{table_name}")
            raise


    def dropTable(self, warehouse, database, schema, table_name):
        try:
            self.execute(f"DROP TABLE {table_name}", warehouse, database, schema=None)
            logging.info(f"Schema creation SUCCESS! for {warehouse}.{schema}.{database}")
        except Exception:
            logging.exception(f"\n\n dropTable failed!")
            raise


    def selectData(self, warehouse, database, schema, table):

        engine = self._engine(warehouse=warehouse, database=database, schema=schema)
        sql = f"SELECT * FROM {table}"
        try:
            cur = engine.cursor()
            cur.execute(sql)
            dataframe = cur.fetch_pandas_all()
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue executing code...")
            raise
        finally:
            cur.close()
        return dataframe