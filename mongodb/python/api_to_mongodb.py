import logging
import requests
import json
import pandas as pd
from pandas import json_normalize
from engine.command import Command
pd.set_option('display.max_columns', None)


class APIToMongoDB(Command):
    def __init__(self, MongoDB_):
        super().__init__("APIToMongoDB")
        self._mongo = MongoDB_


    def _call_internal(self, *args, **kwargs):

        # Get JSON data and upload
        json_data = self._api_collect_json()
        logging.info(json_data)
        database = "APIData"
        collection = "user"
        self._mongo.createAndUpload(database=database, collection=collection, document=json_data)

        # get data from MongoDB
        search = {"gender": "male"}
        document = self._mongo.selectData(database=database, collection=collection, search=search)
        print(document)


    def _api_collect_json(self):
        max_results = 10
        js = []
        for i in range(0, max_results):
            response = requests.get("https://randomuser.me/api/")
            response.encoding = 'utf-8'
            js.append(response.json()['results'][0])
        return js