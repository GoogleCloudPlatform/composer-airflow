#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""This module contains an operator to move data from Hive to MySQL."""
from __future__ import annotations

from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.operator_helpers import context_to_airflow_vars
from airflow.www import utils as wwwutils

if TYPE_CHECKING:
    from airflow.utils.context import Context

# TODO: Remove renderer check when the provider has an Airflow 2.3+ requirement.
MYSQL_RENDERER = 'mysql' if 'mysql' in wwwutils.get_attr_renderer() else 'sql'


class HiveToMySqlOperator(BaseOperator):
    """
    Moves data from Hive to MySQL, note that for now the data is loaded
    into memory before being pushed to MySQL, so this operator should
    be used for smallish amount of data.

    :param sql: SQL query to execute against Hive server. (templated)
    :param mysql_table: target MySQL table, use dot notation to target a
        specific database. (templated)
    :param mysql_conn_id: source mysql connection
    :param hiveserver2_conn_id: Reference to the
        :ref:`Hive Server2 thrift service connection id <howto/connection:hiveserver2>`.
    :param mysql_preoperator: sql statement to run against mysql prior to
        import, typically use to truncate of delete in place
        of the data coming in, allowing the task to be idempotent (running
        the task twice won't double load data). (templated)
    :param mysql_postoperator: sql statement to run against mysql after the
        import, typically used to move data from staging to
        production and issue cleanup commands. (templated)
    :param bulk_load: flag to use bulk_load option.  This loads mysql directly
        from a tab-delimited text file using the LOAD DATA LOCAL INFILE command.
        This option requires an extra connection parameter for the
        destination MySQL connection: {'local_infile': true}.
    :param hive_conf:
    """

    template_fields: Sequence[str] = ('sql', 'mysql_table', 'mysql_preoperator', 'mysql_postoperator')
    template_ext: Sequence[str] = ('.sql',)
    template_fields_renderers = {
        'sql': 'hql',
        'mysql_preoperator': MYSQL_RENDERER,
        'mysql_postoperator': MYSQL_RENDERER,
    }
    ui_color = '#a0e08c'

    def __init__(
        self,
        *,
        sql: str,
        mysql_table: str,
        hiveserver2_conn_id: str = 'hiveserver2_default',
        mysql_conn_id: str = 'mysql_default',
        mysql_preoperator: str | None = None,
        mysql_postoperator: str | None = None,
        bulk_load: bool = False,
        hive_conf: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.mysql_table = mysql_table
        self.mysql_conn_id = mysql_conn_id
        self.mysql_preoperator = mysql_preoperator
        self.mysql_postoperator = mysql_postoperator
        self.hiveserver2_conn_id = hiveserver2_conn_id
        self.bulk_load = bulk_load
        self.hive_conf = hive_conf

    def execute(self, context: Context):
        hive = HiveServer2Hook(hiveserver2_conn_id=self.hiveserver2_conn_id)

        self.log.info("Extracting data from Hive: %s", self.sql)
        hive_conf = context_to_airflow_vars(context)
        if self.hive_conf:
            hive_conf.update(self.hive_conf)
        if self.bulk_load:
            with NamedTemporaryFile() as tmp_file:
                hive.to_csv(
                    self.sql,
                    tmp_file.name,
                    delimiter='\t',
                    lineterminator='\n',
                    output_header=False,
                    hive_conf=hive_conf,
                )
                mysql = self._call_preoperator()
                mysql.bulk_load(table=self.mysql_table, tmp_file=tmp_file.name)
        else:
            hive_results = hive.get_records(self.sql, parameters=hive_conf)
            mysql = self._call_preoperator()
            mysql.insert_rows(table=self.mysql_table, rows=hive_results)

        if self.mysql_postoperator:
            self.log.info("Running MySQL postoperator")
            mysql.run(self.mysql_postoperator)

        self.log.info("Done.")

    def _call_preoperator(self):
        mysql = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        if self.mysql_preoperator:
            self.log.info("Running MySQL preoperator")
            mysql.run(self.mysql_preoperator)
        self.log.info("Inserting rows into MySQL")
        return mysql
