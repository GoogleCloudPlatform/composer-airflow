import logging
import json
import subprocess
import sys
from tempfile import NamedTemporaryFile

from airflow.models import Connection
from airflow.configuration import conf
from airflow import settings

# Adding the Hive python libs to python path
sys.path.insert(0, conf.get('hooks', 'HIVE_HOME_PY'))

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hive_service import ThriftHive

from airflow.hooks.base_hook import BaseHook


class HiveHook(BaseHook):
    def __init__(self,
            hive_conn_id=conf.get('hooks', 'HIVE_DEFAULT_CONN_ID')):
        session = settings.Session()
        db = session.query(
            Connection).filter(
                Connection.conn_id == hive_conn_id)
        if db.count() == 0:
            raise Exception("The conn_id you provided isn't defined")
        else:
            db = db.all()[0]
        self.host = db.host
        self.db = db.schema
        self.hive_cli_params = ""
        try:
            self.hive_cli_params = json.loads(db.extra)['hive_cli_params']
        except:
            pass

        self.port = db.port
        session.commit()
        session.close()

        # Connection to Hive
        self.hive = self.get_hive_client()

    def __getstate__(self):
        # This is for pickling to work despite the thirft hive client not
        # being pickable
        d = dict(self.__dict__)
        del d['hive']
        return d

    def __setstate__(self, d):
        self.__dict__.update(d)
        self.__dict__['hive'] = self.get_hive_client()

    def get_hive_client(self):
        transport = TSocket.TSocket(self.host, self.port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        return ThriftHive.Client(protocol)

    def get_conn(self):
        return self.hive

    def check_for_partition(self, schema, table, partition):
        self.hive._oprot.trans.open()
        partitions = self.hive.get_partitions_by_filter(
            schema, table, partition, 1)
        self.hive._oprot.trans.close()
        if partitions:
            return True
        else:
            return False

    def get_records(self, hql, schema=None):
        self.hive._oprot.trans.open()
        if schema:
            self.hive.execute("USE " + schema)
        self.hive.execute(hql)
        records = self.hive.fetchAll()
        self.hive._oprot.trans.close()
        return [row.split("\t") for row in records]

    def get_pandas_df(self, hql, schema=None):
        import pandas as pd
        self.hive._oprot.trans.open()
        if schema:
            self.hive.execute("USE " + schema)
        self.hive.execute(hql)
        records = self.hive.fetchAll()
        self.hive._oprot.trans.close()
        df = pd.DataFrame([row.split("\t") for row in records])
        return df

    def run(self, hql, schema=None):
        self.hive._oprot.trans.open()
        if schema:
            self.hive.execute("USE " + schema)
        self.hive.execute(hql)
        self.hive._oprot.trans.close()

    def run_cli(self, hql, schema=None):
        if schema:
            hql = "USE {schema};\n{hql}".format(**locals())

        f = NamedTemporaryFile()
        f.write(hql)
        f.flush()
        sp = subprocess.Popen(
                "hive -f {f.name} {self.hive_cli_params}".format(**locals()),
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT)
        all_err = ''
        for line in iter(sp.stdout.readline, ''):
            logging.info(line.strip())
        sp.wait()
        f.close()

        if sp.returncode:
            raise Exception(all_err)

    def max_partition(self, schema, table_name):
        '''
        Returns the maximum value for all partitions in a table. Works only
        for tables that have a single partition key. For subpartitionned
        table, we recommend using signal tables.
        '''
        self.hive._oprot.trans.open()
        table = self.hive.get_table(dbname=schema, tbl_name=table_name)
        if len(table.partitionKeys) == 0:
            raise Exception("The table isn't partitionned")
        elif len(table.partitionKeys) > 1:
            raise Exception(
                "The table is partitionned by multiple columns, "
                "use a signal table!")
        else:
            parts = self.hive.get_partitions(
                db_name=schema, tbl_name=table_name, max_parts=32767)

            self.hive._oprot.trans.close()
            return max([p.values[0] for p in parts])
