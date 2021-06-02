import MySQLdb
from core import settings
from core.models import DatabaseConnection


class MySqlHook(object):

    def __init__(
            self, host=None, login=None, psw=None, db=None, mysql_dbid=None):
        if not mysql_dbid:
            self.host = host
            self.login = login
            self.psw = psw
            self.db = db
        else:
            session = settings.Session()
            db = session.query(
                DatabaseConnection).filter(
                    DatabaseConnection.db_id == mysql_dbid)
            if db.count() == 0:
                raise Exception("The mysql_dbid you provided isn't defined")
            else:
                db = db.all()[0]
            self.host = db.host
            self.login = db.login
            self.psw = db.password
            self.db = db.schema
            session.commit()
            session.close()

    def get_conn(self):
        conn = MySQLdb.connect(
            self.host,
            self.login,
            self.psw,
            self.db)
        return conn

    def get_records(self, sql):
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows

    def get_pandas_df(self, sql):
        import pandas.io.sql as psql
        conn = self.get_conn()
        df = psql.read_sql(sql, con=conn)
        conn.close()
        return df

    def run(self, sql):
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
