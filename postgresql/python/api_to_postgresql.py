import logging
import requests
import json
import pandas as pd
from pandas import json_normalize
from engine.command import Command
pd.set_option('display.max_columns', None)


class APIToPostgresql(Command):
    def __init__(self, Postgresql_):
        super().__init__("APIToPostgresql")
        self._pg = Postgresql_


    def _call_internal(self, *args, **kwargs):

        # Get information from generic API -> dataframe
        data = self._api_collect()

        # Upload to Postgresql
        table_name = "user_table"
        schema = "dev"
        database = "devdb"
        sql = self._pg.createTableSql(data, schema, table_name)  # Create table statement
        self._pg.createSchema(database, schema)  # Create the schema
        self._pg.createTable(database, sql)  # Use the statement and execute.
        logging.info(data)
        self._pg.uploadData(db_name=database, schema=schema, table=table_name, dataframe=data)  # Upload data




    def _api_collect(self):

        max_results = 100
        base = pd.DataFrame()
        for i in range(0, max_results):
            response = requests.get("https://randomuser.me/api/")
            response.encoding = 'utf-8'
            response.json()
            flattened_json = json_normalize(response.json()['results'])
            base = pd.concat([base, flattened_json])

        return base.reset_index(drop=True)