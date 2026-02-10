#
# Copyright 2025 Google LLC
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
import sys
import time

from sqlalchemy import (
    func as sqlfunc,
    text,
    tuple_,
)

from airflow import (
    __version__ as airflow_version,
    settings,
)
from airflow.composer.patches.database_retention.config import Config
from airflow.composer.patches.database_retention.tables import get_table_primary_key

logger = logging.getLogger(__name__)

# The number of times to retry removing a batch of expired rows when an exception is thrown.
MAX_RETRY_ATTEMPTS = 3


def execute_trim(
    retention_days,
    batch_size,
    sleep_between_batches_seconds,
):
    """
    Trim data with provided value for retention period.

    Args:
        retention_days: number of days in retention period.
        batch_size: number of rows in the batch to remove.
        sleep_between_batches_seconds: seconds to sleep between batches.
    """
    signal.signal(signal.SIGINT, _sigint_handler)
    signal.signal(signal.SIGALRM, _sigalrm_handler)

    logger.info(
        (
            "Airflow metadata cleanup started. Airflow version: %s, retention horizon: %d days, "
            "batch size: %d, sleep between batches in seconds: %s."
        ),
        airflow_version,
        retention_days,
        batch_size,
        sleep_between_batches_seconds,
    )

    config = Config(retention_days)

    try:
        with settings.Session() as session:
            _trim_session_table(
                session=session,
                config=config,
                batch_size=batch_size,
                sleep_between_batches_seconds=sleep_between_batches_seconds,
            )
            for table in config.tables:
                _trim_table(
                    session=session,
                    config=config,
                    table=table,
                    batch_size=batch_size,
                    sleep_between_batches_seconds=sleep_between_batches_seconds,
                )
        logger.info("Airflow metadata cleanup completed.")
    except Exception as e:
        logger.error("Airflow metadata cleanup failed. Reason: %s", e)
        sys.exit(1)


def _trim_session_table(
    session,
    config,
    batch_size,
    sleep_between_batches_seconds,
):
    """Delete expired rows from the "session" table."""

    def _trim_batch(_session):
        """Delete a batch of expired rows."""
        sql = f"""
          DELETE FROM session WHERE id IN (
            SELECT id FROM session WHERE expiry < {config.execution_time_str}::date LIMIT {batch_size}
          );
        """
        result = _execute_sql(_session, sql)
        num_removed = result.rowcount
        return num_removed

    _run_trimming_loop(
        session=session,
        table_name="session",
        trim_batch_func=_trim_batch,
        sleep_between_batches_seconds=sleep_between_batches_seconds,
    )


def _trim_table(session, table, config, batch_size, sleep_between_batches_seconds):
    """Delete expired rows from a given table."""

    def _trim_batch(_session):
        """Delete a batch of expired rows."""
        primary_key = get_table_primary_key(table)
        filter_criterion = _prepare_filter_criterion(_session, table, primary_key, config.expiration_datetime)
        table_with_filter_and_limit = _session.query(*primary_key).filter(*filter_criterion).limit(batch_size)
        num_removed = (
            _session.query(table["airflow_db_model"])
            .filter(tuple_(*primary_key).in_(table_with_filter_and_limit))
            .delete(synchronize_session=False)
        )
        return num_removed

    _run_trimming_loop(
        session=session,
        table_name=table["airflow_db_model"].__tablename__,
        trim_batch_func=_trim_batch,
        sleep_between_batches_seconds=sleep_between_batches_seconds,
    )


def _run_trimming_loop(
    session,
    table_name,
    trim_batch_func,
    sleep_between_batches_seconds,
):
    """
    Orchestrate deleting expired rows from the given table in a series of transactions.

    This function executes the given row-trimming function in a loop until no expired rows are left.
    It manages retries and progress logging.

    Args:
        session: database session.
        table_name: the name of the table to trim.
        trim_batch_func: a function removing a batch of expired rows. The function should accept the
        session parameter, use this parameter to delete a batch of expired rows and return the
        number of deleted rows (zero, if all expired rows are already deleted). The function should
        assume it executes within a transaction and should not manage (commit/rollback) the
        transaction.
        sleep_between_batches_seconds: seconds to sleep between batches.
    """
    logger.info("Airflow metadata cleanup started for table '%s'.", table_name)

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
            time.sleep(sleep_between_batches_seconds)
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


def _prepare_filter_criterion(session, table, primary_key, expiration_datetime):
    additional_filter = []

    # Works only for dag_run, because Airflow required last dag_run to be present, otherwise it will
    # schedule this DAG again.
    if table["airflow_db_model"].__tablename__ == "dag_run" and table.get("keep_last", False):
        additional_filter.append(
            tuple_(*primary_key).not_in(
                session.query(sqlfunc.max(tuple_(*primary_key))).group_by(table["airflow_db_model"].dag_id)
            )
        )
    additional_filter.append(table["age_column"] < expiration_datetime)

    return additional_filter


def _execute_sql(session, sql, params=None):
    return session.execute(text(sql), params)


def _sigint_handler(signum, frame):
    """Handle signal in case of interruption."""
    logger.info("Process was interrupted. It didn't finish its work!")
    sys.exit(0)


def _sigalrm_handler(signum, frame):
    """Handle signal in case of interruption."""
    logger.info("Process was time outed. It didn't finish its work!")
    sys.exit(0)
