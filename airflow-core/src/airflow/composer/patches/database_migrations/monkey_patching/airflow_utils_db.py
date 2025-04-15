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

import functools
import logging

from sqlalchemy import Index
from sqlalchemy.engine.reflection import Inspector

from airflow.models.taskinstance import TaskInstance
from airflow.utils import db
from airflow.utils.session import create_session

logger = logging.getLogger(__name__)


def patch():
    db.upgradedb = _composer_db_upgradedb(db.upgradedb)


def _composer_db_upgradedb(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        res = f(*args, **kwargs)
        _apply_composer_migrations()
        return res

    return wrapper


def _apply_composer_migrations():
    logger.info("Applying Composer Airflow migrations")

    with create_session() as session:
        _add_taskinstance_worker_healthcheck_index(session)
        _adjust_length_of_hostname_columns(session)

    logger.info("Composer Airflow migrations applied")


def _add_taskinstance_worker_healthcheck_index(session):
    """Add index for worker healthcheck."""
    connection = session.get_bind()
    inspector = Inspector.from_engine(connection)

    indices = inspector.get_indexes("task_instance")
    for index in indices:
        if index["name"] == "ti_worker_healthcheck":
            return

    index = Index(
        "ti_worker_healthcheck",
        TaskInstance.end_date,
        TaskInstance.hostname,
        TaskInstance.state,
        unique=False,
    )
    index.create(bind=connection)


def _adjust_length_of_hostname_columns(session):
    """
    Adjust length of hostname columns.

    As we use them in index and there's limit for index size, we have to shorten length of these columns.
    """
    session.execute("ALTER TABLE job ALTER COLUMN hostname TYPE VARCHAR(100);")
    session.execute("ALTER TABLE task_instance ALTER COLUMN hostname TYPE VARCHAR(100);")
