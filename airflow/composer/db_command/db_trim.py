#
# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import logging
import signal
from datetime import datetime, timedelta

from sqlalchemy import func as sqlfunc, select, text, tuple_

from airflow import __version__, settings
from airflow.composer.db_command.tables_to_trim import get_table_primary_key, tables_to_trim
from airflow.utils.timezone import make_aware

logger = logging.getLogger(__name__)

execute_sql = lambda session, _sql: session.execute(text(_sql))
execute_sql_with_result = lambda session, _sql: execute_sql(session, _sql).mappings().all()
get_sql_field = lambda _row, field: _row[field]
get_count = lambda _result: get_sql_field(_result[0], "count")

config = None


class Config:
    """Internal data structure for configuration."""

    def __init__(self, retention_days):
        self.execution_time = make_aware(datetime.now())
        self.execution_time_str = self.execution_time.strftime("'%Y-%m-%d %H:%M:%S'")

        self.retention_days = retention_days
        self.expiration_datetime = self.execution_time - timedelta(days=self.retention_days)

        self.tables = tables_to_trim()


def sigint_handler(signum, frame):
    """Handling signal, in case of interruption we should indicate it in logs/metrics."""
    logger.info("Process was interrupted. It didn't finish its work yet!")
    exit(0)


def sigalrm_handler(signum, frame):
    """Handling signal, in case of interruption we should indicate it in logs/metrics."""
    logger.info("Process was timedouted. It didn't finish its work yet!")
    exit(0)


def execute_trim(retention_days):
    """
    Trim looks over data stored in Airflow database and removes data
    older than specific horizon.
    In order to collect metrics properly we are try/catching most of the function. This is in line
    with Python style-guide to create isolation-point and make sure this metric is reported properly.
    """
    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGALRM, sigalrm_handler)
    logger.info(
        f"Trim database command started (AF version: {__version__}, retention horizon: {retention_days})"
    )

    config = Config(retention_days)

    try:
        with settings.Session() as session:
            trim_session_table(session=session, config=config, limit_per_transaction=1000)
            for table in config.tables:
                trim_table(
                    session=session,
                    config=config,
                    table=table,
                    limit_per_transaction=1000,
                )
        logger.info("Database trimming finished!")
    except Exception as e:
        logger.info("Database trimming failed!")
        logger.error(e)


def _log_table_info(table_name, table_size, rows_to_remove, limit):
    """Function used to log information about table."""
    remove_in_transaction_cnt = min(limit, rows_to_remove)
    logger.info(
        f"Table {table_name}: "
        f"total rows: {table_size}, "
        f"trimmable: {rows_to_remove}, "
        f"transaction remove: {remove_in_transaction_cnt}"
    )


def trim_session_table(session, config, limit_per_transaction=1000):
    """Function to delete all sessions in series of transaction."""
    while True:
        expired_sessions_number = trim_session_table_transaction(session, config, limit_per_transaction)
        if expired_sessions_number == 0:
            break
    logger.info("Every expired session was deleted properly")


def trim_session_table_transaction(session, config, limit=1000):
    """Function to delete part of session table in one transaction."""
    sql_session_stmts = prepare_statements(config, limit)

    try:
        expired_sessions_number = get_count(
            execute_sql_with_result(session, sql_session_stmts["select cnt old"])
        )
        sessions_number = get_count(execute_sql_with_result(session, sql_session_stmts["select cnt"]))

        _log_table_info("session", sessions_number, expired_sessions_number, limit)

        execute_sql(session, sql_session_stmts["delete old limit"])

        expired_sessions_number = get_count(
            execute_sql_with_result(session, sql_session_stmts["select cnt old"])
        )

        session.commit()
    except Exception as e:
        logger.error(e)
        expired_sessions_number = 0

    return expired_sessions_number


def prepare_statements(config, limit):
    """Prepare sql statements for session table."""
    statements_dic = {}

    statements_dic["select cnt"] = "SELECT count(*) FROM session;"
    statements_dic[
        "select cnt old"
    ] = f"SELECT count(*) FROM session WHERE expiry < {config.execution_time_str}::date;"
    statements_dic[
        "select old limit"
    ] = f"SELECT id FROM session WHERE expiry < {config.execution_time_str}::date LIMIT {limit};"
    statements_dic[
        "delete old limit"
    ] = f"DELETE FROM session WHERE id IN ({statements_dic['select old limit'][:-1]});"

    return statements_dic


def trim_table(session, table, config, limit_per_transaction=1000):
    """Function to trim given table in series of transaction."""
    while True:
        num_rows_to_remove = trim_table_transaction(session, table, config, limit_per_transaction)
        if num_rows_to_remove == 0:
            break
    logger.info(f"Every expired {table['airflow_db_model'].__tablename__} was deleted properly")


def trim_table_transaction(session, table, config, limit=1000):
    """Function to delete part of a given table in one transaction."""
    primary_key = get_table_primary_key(table)
    filter_criterion = prepare_filter_criterion(session, table, primary_key, config.expiration_datetime)
    try:
        rows_cnt = session.query(table["airflow_db_model"]).count()
        rows_to_remove_cnt = session.query(table["airflow_db_model"]).filter(*filter_criterion).count()

        _log_table_info(table["airflow_db_model"].__tablename__, rows_cnt, rows_to_remove_cnt, limit)

        table_with_filter_and_limit = session.query(*primary_key).filter(*filter_criterion).limit(limit)

        session.query(table["airflow_db_model"]).filter(
            tuple_(*primary_key).in_(table_with_filter_and_limit)
        ).delete(synchronize_session=False)

        rows_to_remove_cnt = session.query(table["airflow_db_model"]).filter(*filter_criterion).count()

        session.commit()
    except Exception as e:
        logger.error(e)
        rows_to_remove_cnt = 0

    return rows_to_remove_cnt


def prepare_filter_criterion(session, table, primary_key, expiration_datetime, skip_age=False):
    """Return filter all restrictions."""
    additional_filter = []

    # Works only for dag_run, because Airflow required last dag_run to be present, otherwise it will
    # schedule this DAG again.
    if table["airflow_db_model"].__tablename__ == "dag_run" and table.get("keep_last", False) is True:
        additional_filter.append(
            tuple_(*primary_key).not_in(
                session.query(sqlfunc.max(tuple_(*primary_key))).group_by(table["airflow_db_model"].dag_id)
            )
        )
    if skip_age:
        return [*additional_filter]
    else:
        return [table["age_column"] < expiration_datetime, *additional_filter]
