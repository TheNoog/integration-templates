import json
import logging

from cassandra.cluster import Cluster

class cassandraCredentials(object):
    @staticmethod
    def get_credentials(credentials_file_path):
        with open(credentials_file_path) as credentials_file:
            credentials = json.load(credentials_file)
            return cassandraCredentials(**credentials)

    def __init__(self, cluster): #password, user, port, host, keyspace):
        # self.user = user
        # self.password = password
        # self.port = port
        # self.host = host
        # self.keyspace = keyspace
        self.cluster = cluster


class Cassandra_(object):
    @staticmethod
    def from_config(config):
        cassandra = Cassandra_(config.cassandra_.credentials_file_path)
        return cassandra

    def __init__(self, credentials_file_path):
        self._credentials_file_path = credentials_file_path

    def _engine(self):
        try:
            credentials = cassandraCredentials.get_credentials(self._credentials_file_path)
            cluster = Cluster([credentials.cluster])
            session = cluster.connect()
            return session
        except Exception:
            logging.exception(f"\n\n ERROR: Missing information to create Cassandra engine.")
            raise


    def execute(self, sql):
        session = self._engine()
        try:
            session.execute(sql)
        except Exception:
            logging.exception(f"\n\n ERROR: There was an issue executing code...")
            raise
        # finally:
            # session.close()
        return f"SQL executed: {sql}."