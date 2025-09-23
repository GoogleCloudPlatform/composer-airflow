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

from airflow.composer.db_command.db_trim import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_SLEEP_BETWEEN_BATCHES_SECONDS,
    execute_trim,
)
from airflow.models import Variable
from airflow.utils import cli as cli_utils

logger = logging.getLogger(__name__)

MINIMAL_RETENTION_DAYS = 30
MAXIMAL_RETENTION_DAYS = 730

MIN_BATCH_SIZE = 100
MAX_BATCH_SIZE = 10000

MIN_SLEEP_TIME = 0.1
MAX_SLEEP_TIME = 60.0

BATCH_SIZE_VARIABLE = "_DB_RETENTION_BATCH_SIZE"
SLEEP_TIME_VARIABLE = "_SLEEP_BETWEEN_BATCHES_SECONDS"


@cli_utils.action_cli(check_db=False)
def trim(args):
    if not args.acknowledge_composer_internal and not args.acknowledge_work_in_progress:
        raise AssertionError(
            "`airflow db trim` is an internal Cloud Composer command. Specify the "
            "--acknowledge-composer-internal flag to suppress this error."
        )

    args.retention_days = int(args.retention_days)
    retention_batch_size = int(Variable.get(BATCH_SIZE_VARIABLE, DEFAULT_BATCH_SIZE))
    retention_sleep = float(Variable.get(SLEEP_TIME_VARIABLE, DEFAULT_SLEEP_BETWEEN_BATCHES_SECONDS))

    # Clamp values to reasonable ranges
    retention_batch_size = max(MIN_BATCH_SIZE, min(retention_batch_size, MAX_BATCH_SIZE))
    retention_sleep = max(MIN_SLEEP_TIME, min(retention_sleep, MAX_SLEEP_TIME))

    if MAXIMAL_RETENTION_DAYS >= args.retention_days >= MINIMAL_RETENTION_DAYS:
        execute_trim(
            args.retention_days,
            batch_size=retention_batch_size,
            sleep_between_batches_seconds=retention_sleep,
        )
    else:
        logger.error(
            "Provided number of days (%s) is not within (%s, %s) range",
            args.retention_days,
            MINIMAL_RETENTION_DAYS,
            MAXIMAL_RETENTION_DAYS,
        )
        raise ValueError(
            f"Retention horizon must be in range({MINIMAL_RETENTION_DAYS}, {MAXIMAL_RETENTION_DAYS})"
        )
