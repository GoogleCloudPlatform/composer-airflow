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
import os

from airflow.composer.patches.database_retention.trim import execute_trim
from airflow.utils import cli as cli_utils

XL_ENV_BATCH_SIZE = 15000
XL_ENV_SLEEP_BETWEEN_BATCHES_SECONDS = 0.2

DEFAULT_BATCH_SIZE = 1000
DEFAULT_SLEEP_BETWEEN_BATCHES_SECONDS = 0.5

MIN_BATCH_SIZE = 1000
MAX_BATCH_SIZE = 100000

MIN_SLEEP_BETWEEN_BATCHES_SECONDS = 0.1
MAX_SLEEP_BETWEEN_BATCHES_SECONDS = 0.5

MINIMAL_RETENTION_DAYS = 30
MAXIMAL_RETENTION_DAYS = 730

COMPOSER_ENVIRONMENT_SIZE = os.environ["COMPOSER_ENVIRONMENT_SIZE"]

logger = logging.getLogger(__name__)


@cli_utils.action_cli(check_db=False)
def trim(args):
    if not args.acknowledge_composer_internal:
        raise AssertionError(
            "`airflow db trim` is an internal Cloud Composer command. Specify the "
            "--acknowledge-composer-internal flag to suppress this error."
        )

    retention_batch_size = args.retention_batch_size or _calculate_retention_batch_size(
        COMPOSER_ENVIRONMENT_SIZE
    )
    retention_sleep = args.retention_sleep or _calculate_retention_sleep(COMPOSER_ENVIRONMENT_SIZE)

    # Cap retention_batch_size and retention_sleep values.
    retention_batch_size = max(MIN_BATCH_SIZE, min(retention_batch_size, MAX_BATCH_SIZE))
    retention_sleep = max(
        MIN_SLEEP_BETWEEN_BATCHES_SECONDS, min(retention_sleep, MAX_SLEEP_BETWEEN_BATCHES_SECONDS)
    )

    if MINIMAL_RETENTION_DAYS <= args.retention_days <= MAXIMAL_RETENTION_DAYS:
        execute_trim(
            args.retention_days,
            batch_size=retention_batch_size,
            sleep_between_batches_seconds=retention_sleep,
        )
    else:
        logger.error(
            "Provided number of days (%d) is not within (%d, %d) range",
            args.retention_days,
            MINIMAL_RETENTION_DAYS,
            MAXIMAL_RETENTION_DAYS,
        )
        raise ValueError(
            f"Retention horizon must be in range({MINIMAL_RETENTION_DAYS}, {MAXIMAL_RETENTION_DAYS})"
        )


def _calculate_retention_batch_size(env_size):
    if env_size == "ENVIRONMENT_SIZE_EXTRA_LARGE":
        return XL_ENV_BATCH_SIZE

    return DEFAULT_BATCH_SIZE


def _calculate_retention_sleep(env_size):
    if env_size == "ENVIRONMENT_SIZE_EXTRA_LARGE":
        return XL_ENV_SLEEP_BETWEEN_BATCHES_SECONDS

    return DEFAULT_SLEEP_BETWEEN_BATCHES_SECONDS
