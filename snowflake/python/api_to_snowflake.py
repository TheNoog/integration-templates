import logging
import requests
import json
import pandas as pd
from pandas import json_normalize
from engine.command import Command
pd.set_option('display.max_columns', None)


class APIToSnowflake(Command):
    def __init__(self, Snowflake_):
        super().__init__("APIToSnowflake")
        self._sf = Snowflake_


    def _call_internal(self, *args, **kwargs):

        # Get information from generic API -> dataframe
        data = self._api_collect()
        logging.info(data)

        # Upload to Snowflake
        warehouse = "COMPUTE_WH"
        database = "DEV_DB"
        schema = "DEV_DB.DEV_SCHEMA"
        table_name = "DEV_USER_TABLE"

        self._sf.createDatabase(warehouse=warehouse, database=database)

        self._sf.createSchema(warehouse=warehouse, 
                              database=database, 
                              schema=schema)

        # Create table
        self._sf.createTableFromDataframe(dataframe=data, 
                                            table_name=table_name, 
                                            warehouse=warehouse, 
                                            database=database, 
                                            schema=schema)

        # Upload the data to the new table
        self._sf.uploadFromDataframeToTable(dataframe=data,
                                            table_name=table_name,
                                            warehouse=warehouse,
                                            database=database, 
                                            schema=schema)



    def _api_collect(self):

        max_results = 10
        base = pd.DataFrame()
        for i in range(0, max_results):
            response = requests.get("https://randomuser.me/api/")
            response.encoding = 'utf-8'
            response.json()
            flattened_json = json_normalize(response.json()['results'])
            base = pd.concat([base, flattened_json])

        # Nested columns from JSON with "outer.inner" names, having "."
        base.columns = base.columns.str.replace(".", "_")

        logging.info("API data collected.")

        return base.reset_index(drop=True)