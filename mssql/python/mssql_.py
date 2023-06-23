import json
import logging
import pandas as pd
import pyodbc
from io import StringIO
import sqlalchemy
import urllib


class MssqlCredentials(object):
    @staticmethod
    def get_credentials(credentials_file_path):
        with open(credentials_file_path) as credentials_file:
            credentials = json.load(credentials_file)
            return MssqlCredentials(**credentials)

    def __init__(self, server, port, database, user, password):
        self.server = server
        self.database = database
        self.user = user
        self.password = password
        self.port = port


class MSSQL_(object):
    @staticmethod
    def from_config(config):
        mssql_ = MSSQL_(config.mssql_.credentials_file_path)
        return mssql_

    def __init__(self, credentials_file_path):
        self._credentials_file_path = credentials_file_path
        self._connection_str = None

    def _engine(self):
        credentials = MssqlCredentials.get_credentials(self._credentials_file_path)
        driver = "{ODBC Driver 18 for SQL Server}"
        connection_str = f"""DRIVER={driver};
                                SERVER={credentials.server};
                                DATABASE={credentials.database};
                                UID={credentials.user};
                                PWD={credentials.password};"""
        quoted = urllib.parse.quote_plus(connection_str)
        engine = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
        return engine


    def execute(self, sql):
        try:
            engine = self._engine()
            connection = engine.connect()
            transaction = connection.begin()
            logging.info(f"Connection established.")
        except Exception:
            logging.exception(f"There was an issue connecting to a database and starting new execute transaction")
            raise

        try:
            row_count = connection.execute(sql)
            row_count = row_count.rowcount
            transaction.commit()
            logging.info(f"SQL Executed.")
        except Exception:
            transaction.rollback()

            logging.exception(f"There was an issue executing sql query against digital data warehouse:\n{sql}")
            raise
        finally:
            connection.close()
        return row_count


    def createSchema(self, schema_name):
        try:
            sql = f"CREATE SCHEMA {schema_name};"
            engine = self._engine()
            engine.execute(sql)
            engine.commit()
            logging.info(f"createSchema complete.")
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue creating schema {schema_name}.")
            raise
        finally:
            engine.close()
        return f"Schema created."


    def createTable(self, sql):
        try:
            engine = self._engine()
            engine.execute(sql)
            engine.commit()
            logging.info(f"createTable complete.")
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue creating the table for database.")
            raise
        finally:
            engine.close()
        return f"Table created."


    def dtype_mapping(self):
        return {'object' : 'VARCHAR(MAX)',
            'int64' : 'BIGINT',
            'float64' : 'FLOAT',
            'datetime64' : 'DATETIME',
            'bool' : 'BOOLEAN',
            'timedelta[ns]' : 'DATETIME'}

    def createTableSql(self, dataframe, schema, table_name):
        create_table_sql = f"CREATE TABLE {schema}.{table_name}("
        try:
            for i in range(0, len(list(dataframe.columns))):
                attr = list(dataframe.columns)[i]
                d_type = list(dataframe.dtypes)[i]
                dmap = self.dtype_mapping()
                create_table_sql = create_table_sql + f"\"{attr}\" {dmap[str(d_type)]}, "
                
                if (i+1) == len(list(dataframe.columns)):
                    create_table_sql = create_table_sql[:-2] + f");"

            logging.info(create_table_sql)
        except Exception:
            logging.error(f"\n\n ERROR: There was an issue making the create table statement for {schema}.{table_name}")
            logging.info(create_table_sql)
            raise

        return create_table_sql



    def append(self, data, schema, table):
        try:
            engine = self._engine()
            connection = engine.connect()
            transaction = None
        except Exception:
            logging.exception(f"There was an issue connecting to a database")
            raise

        try:
            if len(data) == 0:
                logging.info(f"No data. Nothing to append to {schema}.{table}")
                return
            transaction = connection.begin()

            if "id" in data and data["id"].count() < len(data):
                data = data.drop("id", axis=1)

            data.to_sql(
                f"{table}",
                self._engine(),
                schema=schema,
                chunksize=10000,
                if_exists="append",
                index=False,
                method=None,
            )
            transaction.commit()
            logging.info(f"Appended {len(data)} rows to {schema}.{table} table")
        except Exception:
            if transaction is not None:
                transaction.rollback()
            logging.exception(
                f"There was an issue trying to append {len(data)} rows to {schema}.{table}")
            raise
        finally:
            connection.close()