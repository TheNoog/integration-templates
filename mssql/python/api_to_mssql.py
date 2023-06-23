import logging
import requests
import json
import pandas as pd
from pandas import json_normalize
from engine.command import Command
pd.set_option('display.max_columns', None)


class APIToMSSQL(Command):
    def __init__(self, MSSQL_):
        super().__init__("APIToMSSQL")
        self._ms = MSSQL_


    def _call_internal(self, *args, **kwargs):

        # Get information from generic API -> dataframe
        data = self._api_collect()

        # Upload to MSSQL
        schema = "test"
        table_name = "fake_users"
        self._ms.execute("SELECT @@version;")
        self._ms.createSchema(schema)
        sql = self._ms.createTableSql(dataframe=data, schema=schema, table_name=table_name)
        self._ms.createTable(sql)
        self._ms.append(data=data, schema=schema, table=table_name)



    def _api_collect(self):

        max_results = 3
        base = pd.DataFrame()
        for i in range(0, max_results):
            response = requests.get("https://randomuser.me/api/")
            response.encoding = 'utf-8'
            response.json()
            flattened_json = json_normalize(response.json()['results'])
            base = pd.concat([base, flattened_json])

        return base.reset_index(drop=True)