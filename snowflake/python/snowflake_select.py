import logging
import requests
import json
import pandas as pd
from pandas import json_normalize
from engine.command import Command
pd.set_option('display.max_columns', None)


class SnowflakeSelect(Command):
    def __init__(self, Snowflake_):
        super().__init__("SnowflakeSelect")
        self._sf = Snowflake_


    def _call_internal(self, *args, **kwargs):

        warehouse = "COMPUTE_WH"
        database = "TESTDB_MG"
        schema = "TESTSCHEMA_MG"
        table_name = "TEST_FAKE_USERS"

        data = self._sf.selectData(warehouse=warehouse,
                                   database=database,
                                   schema=schema,
                                   table=table_name)

        logging.info(data)