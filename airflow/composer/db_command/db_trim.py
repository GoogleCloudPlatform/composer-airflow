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
import time
import signal
from datetime import datetime, timedelta

from sqlalchemy import func as sqlfunc, select, text, tuple_

from airflow import __version__, settings
from airflow.composer.db_command.tables_to_trim import get_table_primary_key, tables_to_trim
from airflow.utils.timezone import make_aware

logger = logging.getLogger(__name__)


def execute_sql(session, _sql, params=None):
    return session.execute(text(_sql), params)


def execute_sql_with_result(session, _sql, params=None):
    return execute_sql(session, _sql, params).mappings().all()


get_sql_field = lambda _row, field: _row[field]
get_count = lambda _result: get_sql_field(_result[0], "count")

config = None

# The number or seconds to sleep between removing batches of expired rows. Lowering the number makes
# the removal faster, but also increases the database CPU usage.
SLEEP_BETWEEN_BATCHES_SECONDS = 0.5

# The number of times to retry removing a batch of expired rows when an exception is thrown.
MAX_RETRY_ATTEMPTS = 3


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
    """
    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGALRM, sigalrm_handler)
    logger.info(
        f"Airflow metadata cleanup started - Airflow version: {__version__}, retention horizon: {retention_days} days."
    )

    config = Config(retention_days)

    try:
        with settings.Session() as session:
            trim_session_table(session=session, config=config, batch_size=1000)
            for table in config.tables:
                trim_table(
                    session=session,
                    config=config,
                    table=table,
                    batch_size=1000,
                )
        logger.info("Airflow metadata cleanup completed.")
    except Exception as e:
        logger.error("Airflow metadata cleanup failed. Reason: %s", e)
        exit(1)


def trim_session_table(session, config, batch_size=1000):
    """Deletes expired rows from the 'session' table in a series of transactions."""

    def trim_batch(_session):
        """Deletes a batch of expired rows."""
        sql = f"""
          DELETE FROM session WHERE id IN (
            SELECT id FROM session WHERE expiry < {config.execution_time_str}::date LIMIT {batch_size}
          );
        """
        result = execute_sql(_session, sql)
        num_removed = result.rowcount
        return num_removed

    estimated_num_expired_rows = estimate_result_count(
        session=session,
        _sql=f"SELECT id FROM session WHERE expiry < {config.execution_time_str}::date;",
    )
    run_trimming_loop(
        session=session,
        table_name="session",
        estimated_num_expired_rows=estimated_num_expired_rows,
        trim_batch_func=trim_batch,
    )


def trim_table(session, table, config, batch_size=1000):
    """Deleted expired rows from a given table in a series of transactions."""

    def trim_batch(_session):
        """Deletes a batch of expired rows."""
        primary_key = get_table_primary_key(table)
        filter_criterion = prepare_filter_criterion(_session, table, primary_key, config.expiration_datetime)
        table_with_filter_and_limit = _session.query(*primary_key).filter(*filter_criterion).limit(batch_size)
        num_removed = (
            _session.query(table["airflow_db_model"])
            .filter(tuple_(*primary_key).in_(table_with_filter_and_limit))
            .delete(synchronize_session=False)
        )
        return num_removed

    estimated_num_expired_rows = estimate_result_count(
        session=session,
        _sql=select_expired_rows_sql(session, table, config),
    )
    run_trimming_loop(
        session=session,
        table_name=table["airflow_db_model"].__tablename__,
        estimated_num_expired_rows=estimated_num_expired_rows,
        trim_batch_func=trim_batch,
    )


def run_trimming_loop(session, table_name, estimated_num_expired_rows, trim_batch_func):
    """Orchestrates deleting expired rows from the given table in a series of transactions.

    This function executes the given row-trimming function in a loop until no expired rows are left.
    It manages retries and progress logging.

    Args:
      table_name: The name of the table being trimmed.
      estimated_num_expired_rows: Estimated total number of expired rows.
      trim_batch_func: A function removing a batch of expired rows. The function should accept the
        session parameter, use this parameter to delete a batch of expired rows and return the
        number of deleted rows (zero, if all expired rows are already deleted). The function should
        assume it executes within a transaction and should not manage (commit/rollback) the
        transaction.
    """
    logger.info(
        "Airflow metadata cleanup started for table '%s'. Estimated number of expired rows to remove is %s.",
        table_name,
        format(estimated_num_expired_rows, ","),
    )
    total_num_removed_rows = 0
    attempt_num = 1
    while True:
        try:
            num_removed_rows = trim_batch_func(session)
            if num_removed_rows == 0:
                break
            session.commit()
            # The commit was successful, so reset the retry attempt counter.
            attempt_num = 1
            total_num_removed_rows += num_removed_rows
            logger.info(
                "Airflow metadata cleanup in progress for table '%s'. Removed %s expired rows.",
                table_name,
                format(total_num_removed_rows, ","),
            )
            time.sleep(SLEEP_BETWEEN_BATCHES_SECONDS)
        except Exception as e:
            session.rollback()
            logger.warning(
                "Airflow metadata cleanup error for table '%s' (attempt %d/%d). Error: %s.",
                table_name,
                attempt_num,
                MAX_RETRY_ATTEMPTS,
                e,
            )
            attempt_num += 1
            if attempt_num > MAX_RETRY_ATTEMPTS:
                raise e
    logger.info(
        "Airflow metadata cleanup completed for table '%s'. Removed a total of %s rows.",
        table_name,
        format(total_num_removed_rows, ","),
    )


def select_expired_rows_sql(session, table, config):
    """Returns an SQL query string that selects all expired rows from the given table."""
    filter_criterion = prepare_filter_criterion(
        session,
        table,
        get_table_primary_key(table),
        config.expiration_datetime,
    )
    query = session.query(table["airflow_db_model"]).filter(*filter_criterion)
    return str(query.statement.compile(compile_kwargs={"literal_binds": True}))


def estimate_result_count(session, _sql):
    """Returns an estimated number of rows that a given SQL query would return."""

    # Create a temporary database function that estimates the count of rows returned by the given
    # query. Based on https://wiki.postgresql.org/wiki/Count_estimate.
    #
    # The pg_temp schema is per-connection and is dropped when the connection is closed, so the
    # temporary function doesn't need to be cleaned up.

    # PostgreSQL Database Management System
    # (formerly known as Postgres, then as Postgres95)
    #
    # Portions Copyright © 1996-2025, The PostgreSQL Global Development Group
    #
    # Portions Copyright © 1994, The Regents of the University of California
    #
    # Permission to use, copy, modify, and distribute this software and its documentation for any
    # purpose, without fee, and without a written agreement is hereby granted, provided that the
    # above copyright notice and this paragraph and the following two paragraphs appear in all
    # copies.
    #
    # IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT,
    # SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF THE USE
    # OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED
    # OF THE POSSIBILITY OF SUCH DAMAGE.
    #
    # THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED
    # TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
    # SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO
    # OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
    execute_sql(
        session,
        """
        CREATE OR REPLACE FUNCTION pg_temp.count_estimate(
            query text
        ) RETURNS integer LANGUAGE plpgsql AS $$
        DECLARE
            plan jsonb;
        BEGIN
            EXECUTE FORMAT('EXPLAIN (FORMAT JSON) %s', query) INTO plan;
            RETURN plan->0->'Plan'->'Plan Rows';
        END;
        $$;
    """,
    )
    # Pass the input query string to the estimation function.
    estimate_row_count_sql = "SELECT pg_temp.count_estimate(:query_sql) AS count;"
    estimate_row_count_params = {"query_sql": _sql}
    result = execute_sql_with_result(session, estimate_row_count_sql, estimate_row_count_params)
    return get_count(result)


def prepare_filter_criterion(session, table, primary_key, expiration_datetime, skip_age=False):
    """Return filter all restrictions."""
    additional_filter = []

    # Works only for dag_run, because Airflow required last dag_run to be present, otherwise it will
    # schedule this DAG again.
    if table["airflow_db_model"].__tablename__ == "dag_run" and table.get("keep_last", False):
        additional_filter.append(
            tuple_(*primary_key).not_in(
                session.query(sqlfunc.max(tuple_(*primary_key))).group_by(table["airflow_db_model"].dag_id)
            )
        )
    if skip_age:
        return [*additional_filter]
    else:
        return [table["age_column"] < expiration_datetime, *additional_filter]
