import pymongo
from pymongo import MongoClient
import json
import logging
import pandas as pd
import pandas.io.sql as sqlio
from io import StringIO
pd.set_option('display.max_columns', None)

class mongodbCredentials(object):
    @staticmethod
    def get_credentials(credentials_file_path):
        with open(credentials_file_path) as credentials_file:
            credentials = json.load(credentials_file)
            return mongodbCredentials(**credentials)

    def __init__(self, server, port, database, user, password):
        self.server = server
        self.port = port
        self.database = database
        self.user = user
        self.password = password

class MongoDB_(object):
    @staticmethod
    def from_config(config):
        mongodb = MongoDB_(config.mongodb_.credentials_file_path)
        return mongodb

    def __init__(self, credentials_file_path):
        self._credentials_file_path = credentials_file_path

    def _engine(self):
        credentials = mongodbCredentials.get_credentials(self._credentials_file_path)
        return MongoClient(f"mongodb://{credentials.server}:{credentials.port}/")

    def createAndUpload(self, database=None, collection=None, document=None):
        # Examples - https://pythonexamples.org/python-mongodb-create-database/
        try:
            client = self._engine()
            logging.info(f"Length of document = {len(document)}")
            if len(document) == 2:
                # database will only be created once 1+ documents are added.
                db = client[f"{database}"]
                col = db[f"{collection}"]
                x = col.insert_one(document)  # insert a document to the collection
            elif len(document) > 2:
                with client:
                    db = client[database]   # Using explicit "database" to send the data and not the actual database (colleciton).
                    db[collection].insert_many(document)
            # list the databases
            for db in client.list_databases():
                print(db)
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue creating the database {database}.")
            raise
        return f"Database {database}, Collection {collection} created. 1 Document inserted."

    def listDBs(self):
        # Examples - https://pythonexamples.org/python-mongodb-create-database/
        try:
            client = self._engine()
            # list the databases
            for db in client.list_databases():
                print(db)
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue creating the database {database}.")
            raise
        return f"Database {database}, Collection {collection} created. 1 Document inserted."


    def selectData(self, database=None, collection=None, search=None):
        # Examples - https://www.mongodb.com/languages/python
        try:
            client = self._engine()
            db = client[f"{database}"]
            col = db[f"{collection}"]

            item_details = col.find(search)
            logging.info(f"item_details = {item_details}")

            for item in item_details:
                logging.info(f"item = {item}")
                print(item)

        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue creating the database {database}.")
            raise
        return item_details