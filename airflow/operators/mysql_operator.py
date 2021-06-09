import logging

from airflow.hooks import MySqlHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class MySqlOperator(BaseOperator):
    """
    Executes sql code in a specific mysql database.
    """

    __mapper_args__ = {
        'polymorphic_identity': 'MySqlOperator'
    }
    template_fields = ('sql',)
    template_ext = ('.sql',)

    @apply_defaults
    def __init__(self, sql, mysql_dbid, *args, **kwargs):
        """
        Parameters:
        mysql_dbid: reference to a specific mysql database
        sql: the sql code you to be executed
        """
        super(MySqlOperator, self).__init__(*args, **kwargs)

        self.hook = MySqlHook(mysql_dbid=mysql_dbid)
        self.sql = sql

    def execute(self, execution_date):
        logging.info('Executing: ' + self.sql)
        self.hook.run(self.sql)
