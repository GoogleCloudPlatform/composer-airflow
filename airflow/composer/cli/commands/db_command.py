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
import os

from airflow.composer.db_command.db_trim import execute_trim
from airflow.utils import cli as cli_utils

logger = logging.getLogger(__name__)

MINIMAL_RETENTION_DAYS = 30
MAXIMAL_RETENTION_DAYS = 730

MIN_BATCH_SIZE = 1000
MAX_BATCH_SIZE = 100000

MIN_SLEEP_BETWEEN_BATCHES_SECONDS = 0.1
MAX_SLEEP_BETWEEN_BATCHES_SECONDS = 0.5

XL_ENV_SLEEP_BETWEEN_BATCHES_SECONDS = 0.2
XL_ENV_BATCH_SIZE = 15000

# Default number or seconds to sleep between removing batches of expired rows. Lowering the number makes
# the removal faster, but also increases the database CPU usage.
DEFAULT_SLEEP_BETWEEN_BATCHES_SECONDS = 0.5

# Default number of rows do delete in a single batch. Increasing the number makes
# the removal faster, but also increases the database CPU usage.
DEFAULT_BATCH_SIZE = 1000


@cli_utils.action_cli(check_db=False)
def trim(args):
    def _calculate_retention_batch_size(env_size):
        if env_size == "XL":
            return XL_ENV_BATCH_SIZE
        return DEFAULT_BATCH_SIZE

    def _calculate_retention_sleep(env_size):
        if env_size == "XL":
            return XL_ENV_SLEEP_BETWEEN_BATCHES_SECONDS
        return DEFAULT_SLEEP_BETWEEN_BATCHES_SECONDS

    if not args.acknowledge_composer_internal and not args.acknowledge_work_in_progress:
        raise AssertionError(
            "`airflow db trim` is an internal Cloud Composer command. Specify the "
            "--acknowledge-composer-internal flag to suppress this error."
        )

    args.retention_days = int(args.retention_days)

    env_size = os.environ.get("COMPOSER_ENVIRONMENT_SIZE")

    retention_batch_size = args.retention_batch_size or _calculate_retention_batch_size(env_size)
    retention_sleep = args.retention_sleep or _calculate_retention_sleep(env_size)

    retention_batch_size = max(MIN_BATCH_SIZE, min(retention_batch_size, MAX_BATCH_SIZE))
    retention_sleep = max(
        MIN_SLEEP_BETWEEN_BATCHES_SECONDS, min(retention_sleep, MAX_SLEEP_BETWEEN_BATCHES_SECONDS)
    )

    if MAXIMAL_RETENTION_DAYS >= args.retention_days >= MINIMAL_RETENTION_DAYS:
        execute_trim(
            args.retention_days,
            batch_size=retention_batch_size,
            sleep_between_batches_seconds=retention_sleep,
        )
    else:
        logger.error(
            f"Provided number of days ({args.retention_days}) is not within "
            f"({MINIMAL_RETENTION_DAYS}, {MAXIMAL_RETENTION_DAYS}) range"
        )
        raise ValueError(
            f"Retention horizon must be in range({MINIMAL_RETENTION_DAYS}, {MAXIMAL_RETENTION_DAYS})"
        )
