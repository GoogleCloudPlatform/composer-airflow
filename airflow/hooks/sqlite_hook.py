import sqlite3

from airflow.hooks.dbapi_hook import DbApiHook


class SqliteHook(DbApiHook):

    """
    Interact with SQLite.
    """

    conn_name_attr = 'sqlite_conn_id'
    default_conn_name = 'sqlite_default'
    supports_autocommit = True

    def get_conn(self):
        """
        Returns a sqlite connection object
        """
        conn = self.get_connection(self.conn_id_name)
        conn = sqlite3.connect(conn.host)
        return conn
