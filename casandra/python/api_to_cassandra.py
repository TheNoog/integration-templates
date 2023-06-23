import logging
import requests
import json
import pandas as pd
from pandas import json_normalize
from engine.command import Command
pd.set_option('display.max_columns', None)


class APIToCassandra(Command):
    def __init__(self, Cassandra_):
        super().__init__("APIToCassandra")
        self._cass = Cassandra_


    def _call_internal(self, *args, **kwargs):

        # Get information from generic API -> dataframe
        data = self._api_collect()
        logging.info(data.head())


        SQL1 = """CREATE KEYSPACE demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}"""
        SQL2 = """CREATE TABLE demo.users (lastname text PRIMARY KEY, firstname text, email text); """

        #self._cass.execute(SQL1)
        self._cass.execute(SQL2)



    def _api_collect(self):

        max_results = 3
        base = pd.DataFrame()
        for i in range(0, max_results):
            response = requests.get("https://randomuser.me/api/")
            response.encoding = 'utf-8'
            response.json()
            flattened_json = json_normalize(response.json()['results'])
            base = pd.concat([base, flattened_json])

        # A dataframe of the API results collected.
        return base.reset_index(drop=True)
