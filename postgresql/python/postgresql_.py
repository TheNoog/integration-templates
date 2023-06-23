import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import json
import logging
import urllib
import dotenv
import numpy as np
import pandas as pd
import pandas.io.sql as sqlio
from io import StringIO
pd.set_option('display.max_columns', None)

class postgresCredentials(object):
    @staticmethod
    def get_credentials(credentials_file_path):
        with open(credentials_file_path) as credentials_file:
            credentials = json.load(credentials_file)
            return postgresCredentials(**credentials)

    def __init__(self, server, port, database, user, password):
        self.server = server
        self.port = port
        self.dbname = database
        self.user = user
        self.password = password

    def get_connection_string(self):
        connection_str = "host='{}' port={} dbname='{}' user={} password={}".format(self.server, self.port, self.dbname, self.user, self.password)
        return connection_str


class Postgresql_(object):
    @staticmethod
    def from_config(config):
        postgres = Postgresql_(config.postgresql_.database, config.postgresql_.credentials_file_path)
        return postgres

    def __init__(self, database, credentials_file_path):
        self._database = database
        self._credentials_file_path = credentials_file_path
        self._connection_str = None

    def _engine(self, db_name=None, options=None):

        credentials = postgresCredentials.get_credentials(self._credentials_file_path)

        if db_name == None:
            db_name = credentials.dbname

        return psycopg2.connect(host=credentials.server,
                                        port=credentials.port,
                                        database=db_name,
                                        user=credentials.user,
                                        password=credentials.password,
                                        options=options)


    def execute(self, sql, db_name=None):
        try:
            engine = self._engine(db_name)
            engine.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = engine.cursor()
            cursor.execute(sql)
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue creating the database {db_name}.")
            raise
        finally:
            engine.close()
        return f"SQL executed: {sql}."


    def listDBs(self):
        try:
            sql = "SELECT datname FROM pg_database WHERE datistemplate = false AND datname NOT IN ('postgres', 'azure_maintenance', 'azure_sys');"
            result = sqlio.read_sql_query(sql, self._engine(db_name=None))
            return result
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue collecting the list of databases.")
            raise

    def listSchemas(self, db_name): 
        try:
            sql = """SELECT * 
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog') 
                AND table_name NOT IN ('pg_stat_statements', 'pg_buffercache') 
                ORDER BY table_schema, table_name;
            """
            result = sqlio.read_sql_query(sql, self._engine(db_name))
            return result
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue listing all schemas for the database {db_name}.")
            raise

    def listTables(self, db_name): 
        try:
            sql = """SELECT * 
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog') 
                AND table_name NOT IN ('pg_stat_statements', 'pg_buffercache') 
                AND table_type NOT IN ('VIEW')
                ORDER BY table_schema, table_name;
            """
            result = sqlio.read_sql_query(sql, self._engine(db_name))
            return result
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue listing all tables for the database {db_name}.")
            raise

    def listStoredProcs(self):
        try:
            sql = """SELECT n.nspname as schema, p.proname as procedure 
                        FROM pg_proc p JOIN pg_namespace n on p.pronamespace = n.oid 
                        WHERE n.nspname not in ('pg_catalog', 'information_schema') AND p.prokind = 'p';"""
            result = sqlio.read_sql_query(sql, self._engine())
            return result
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue listing all stored procedures for the database.")
            raise

    def listViews(self, db_name):
        try:
            sql = "SELECT * FROM INFORMATION_SCHEMA.views WHERE view_definition IS NOT NULL;"
            result = sqlio.read_sql_query(sql, self._engine(db_name))
            return result
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue listing all views for the database.")
            raise

    def listFunctions(self):
        try:
            sql = """SELECT routines.routine_name, parameters.data_type, parameters.ordinal_position
                    FROM information_schema.routines
                        LEFT JOIN information_schema.parameters ON routines.specific_name=parameters.specific_name
                    ORDER BY routines.routine_name, parameters.ordinal_position
                """
            result = sqlio.read_sql_query(sql, self._engine())
            return result
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue listing all functions for the database.")
            raise


    def createDB(self, database):
        try:
            engine = self._engine(db_name=None)
            engine.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = engine.cursor()
            sql = f"CREATE DATABASE {database};"
            cursor.execute(sql)
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue creating the database {database}.")
            raise
        finally:
            engine.close()
        return f"Database {database} created."


    def dropDB(self, database):
        try:
            engine = self._engine(db_name=database)
            engine.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = engine.cursor()
            sql = f"DROP DATABASE {database};"
            cursor.execute(sql)
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue creating the database {database}.")
            raise
        finally:
            engine.close()
        return f"Database {database} created."


    def createSchema(self, database, schema):
        try:
            engine = self._engine(db_name=database)
            engine.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = engine.cursor()
            sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
            cursor.execute(sql)
            logging.info(f"createSchema complete for {database}.{schema}")
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue creating the database {database}.")
            raise
        finally:
            engine.close()
        return f"Database {database}.{schema} created."


    def dropSchema(self, database, schema):
        try:
            engine = self._engine(db_name=database)
            engine.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = engine.cursor()
            sql = f"DROP SCHEMA IF NOT EXISTS {schema};"
            cursor.execute(sql)
            logging.info(f"dropSchema complete for {database}.{schema}")
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue creating the database {database}.")
            raise
        finally:
            engine.close()
        return f"Database {database}.{schema} created."


    def createTable(self, database, sql):
        try:
            engine = self._engine(db_name=database)
            engine.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = engine.cursor()
            cursor.execute(sql)
            logging.info(f"createTable complete for {database}")
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue creating the table for database {database}.")
            raise
        finally:
            engine.close()
        return f"Table created for {database}."


    def dtype_mapping(self):
        return {'object' : 'TEXT',
            'int64' : 'BIGINT',
            'float64' : 'NUMERIC',
            'datetime64' : 'TIMESTAMP',
            'bool' : 'BOOLEAN',
            'timedelta[ns]' : 'INTERVAL'}

    def createTableSql(self, dataframe, schema, table_name):  
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {schema}.{table_name}("
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


    def dropTable(self, database, schema, table):
        try:
            engine = self._engine(db_name=database)
            engine.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = engine.cursor()
            cursor.execute(f"DROP TABLE {schema}.{table};")
            logging.info(f"dropTable complete for {database}.{schema}.{table}")
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue dropping the table {database}.{schema}.{table}.")
            raise
        finally:
            engine.close()
        return f"Table {database}.{schema}.{table} dropped."


    def selectTable(self, db_name, schema, table):
        try:
            sql = f"SELECT * FROM {schema}.{table};"
            result = sqlio.read_sql_query(sql, self._engine(db_name))
            return result
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue listing all tables for the database {db_name}.")
            raise


    def selectTableShape(self, db_name, schema, table):
        try:
            sql = f"""SELECT table_catalog, table_schema, table_name, column_name, data_type, is_nullable 
                    FROM information_schema.columns
                    WHERE table_schema = '{schema}' AND table_name = '{table}';"""
            result = sqlio.read_sql_query(sql, self._engine(db_name))
            return result
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue listing all tables for the database {db_name}.")
            raise


    def uploadData(self, db_name, schema, table, dataframe):
        try:
            if schema == "raw":
                schema = "\"raw\""
            engine = self._engine(db_name, options=f"-c search_path={schema}")
            buffer = StringIO()
            dataframe.to_csv(buffer, index=False, header=False, sep='\t')
            buffer.seek(0)
            cursor = engine.cursor()
            cursor.copy_from(buffer, table, sep="\t")
            engine.commit()
            logging.info(f"Data uploaded successfully for {db_name}.{schema}.{table}")
        except Exception:
            logging.exception(f"\n\n ERROR: TABLE-LEVEL ISSUE: There was an issue uploading to table {table}. Dataframe length = {len(dataframe)}")
            logging.exception(dataframe.head())
            logging.exception(dataframe.dtypes)
            engine.rollback()
            engine.close()
            #raise
            return False
        finally:
            engine.close()
        return True


    def createView(self, db_name, schema, view, sql):
        try:
            if schema == "raw":
                schema = "\"raw\""
            engine = self._engine(db_name, options=f"-c search_path={schema}")
            sql_string = f"CREATE OR REPLACE VIEW {schema}.{view} AS " + sql
            cursor = engine.cursor()
            cursor.execute(sql_string)
            engine.commit()
            return f"View {db_name}.{schema}.{view} created."
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue creating the view {db_name}.{schema}.{view}")
            raise


    def dropView(self, database, schema, view):
        try:
            engine = self._engine(db_name=database)
            engine.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = engine.cursor()
            cursor.execute(f"DROP VIEW {schema}.{view};")
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue dropping the table {database}.{schema}.{view}.")
            raise
        finally:
            engine.close()
        return f"Table {database}.{schema}.{view} dropped."