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
"""Microsoft SQLServer hook module"""

import warnings

import pymssql

from airflow.hooks.dbapi_hook import DbApiHook


class MsSqlHook(DbApiHook):
    """Interact with Microsoft SQL Server."""

    conn_name_attr = 'mssql_conn_id'
    default_conn_name = 'mssql_default'
    supports_autocommit = True

    def __init__(self, *args, **kwargs) -> None:
        warnings.warn(
            (
                "This class is deprecated and will be removed in Airflow 2.0.\n"
                "pymssql is discontinued.  See https://github.com/pymssql/pymssql/issues/668.\n"
                "Please use `airflow.providers.odbc.hooks.odbc.OdbcHook`"
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)

    def get_conn(
        self,
    ) -> pymssql.connect:  # pylint: disable=protected-access  # pylint: disable=c-extension-no-member
        """Returns a mssql connection object"""
        conn = self.get_connection(
            self.mssql_conn_id  # type: ignore[attr-defined]  # pylint: disable=no-member
        )
        # pylint: disable=c-extension-no-member
        conn = pymssql.connect(
            server=conn.host,
            user=conn.login,
            password=conn.password,
            database=self.schema or conn.schema,
            port=conn.port,
        )
        return conn

    def set_autocommit(
        self,
        conn: pymssql.connect,  # pylint: disable=c-extension-no-member
        autocommit: bool,
    ) -> None:
        conn.autocommit(autocommit)

    def get_autocommit(self, conn: pymssql.connect):  # pylint: disable=c-extension-no-member
        return conn.autocommit_state
