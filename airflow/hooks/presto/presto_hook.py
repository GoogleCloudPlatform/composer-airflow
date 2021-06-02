import subprocess
import StringIO

from airflow import settings
from airflow.models import DatabaseConnection
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.presto.presto_client import PrestoClient

class PrestoException(Exception):
    pass

class PrestoHook(BaseHook):
    """
    Interact with Presto!
    """
    def __init__(self, presto_dbid=settings.PRESTO_DEFAULT_DBID):
        session = settings.Session()
        db = session.query(
            DatabaseConnection).filter(
                DatabaseConnection.db_id == presto_dbid)
        if db.count() == 0:
            raise Exception("The presto_dbid you provided isn't defined")
        else:
            db = db.all()[0]
        self.host = db.host
        self.db = db.schema
        self.port = db.port
        session.commit()
        session.close()

        self.client = PrestoClient(
            self.host, in_port=self.port, in_catalog=self.db,
            in_user='airflow')

    def get_records(self, hql, schema="default"):
        if self.client.runquery(hql, schema):
            return self.client.getdata()
        else:
            raise PrestoException(self.client.getlasterrormessage())

    def get_pandas_df(self, hql, schema="default"):
        import pandas
        client = self.client
        if client.runquery(hql, schema):
            data = client.getdata()
            df = pandas.DataFrame(data)
            df.columns = [c['name'] for c in client.getcolumns()]
            return df
        else:
            raise PrestoException(self.client.getlasterrormessage())


