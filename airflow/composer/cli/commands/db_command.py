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

from airflow.composer.db_command.db_trim import execute_trim
from airflow.utils import cli as cli_utils

logger = logging.getLogger(__name__)

MINIMAL_RETENTION_DAYS = 30
MAXIMAL_RETENTION_DAYS = 730


@cli_utils.action_cli(check_db=False)
def trim(args):
    if not args.acknowledge_composer_internal and not args.acknowledge_work_in_progress:
        raise AssertionError(
            "`airflow db trim` is an internal Cloud Composer command. Specify the "
            "--acknowledge-composer-internal flag to suppress this error."
        )

    args.retention_days = int(args.retention_days)
    if MAXIMAL_RETENTION_DAYS >= args.retention_days >= MINIMAL_RETENTION_DAYS:
        execute_trim(args.retention_days)
    else:
        logger.error(
            f"Provided number of days ({args.retention_days}) is not within "
            f"({MINIMAL_RETENTION_DAYS}, {MAXIMAL_RETENTION_DAYS}) range"
        )
        raise ValueError(
            f"Retention horizon must be in range({MINIMAL_RETENTION_DAYS}, {MAXIMAL_RETENTION_DAYS})"
        )
