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

from airflow.cli import cli_config

ARG_DB_TRIM_RETENTION_DAYS = cli_config.Arg(
    ("--retention-days",),
    help="Data older than this number of days will get trimmed",
    type=int,
    required=True,
)
ARG_DB_TRIM_COMPOSER_INTERNAL = cli_config.Arg(
    ("--acknowledge-composer-internal",),
    help="Acknowledge that this is an internal Cloud Composer command",
    action="store_true",
)
ARG_DB_TRIM_BATCH_SIZE = cli_config.Arg(
    ("--retention-batch-size",),
    help="Number of database rows that will get trimmed per batch",
    type=int,
    required=False,
)
ARG_DB_TRIM_SLEEP = cli_config.Arg(
    ("--retention-sleep",),
    help="Sleep time between batches of data trim",
    type=float,
    required=False,
)


def patch():
    db_command_ind = None
    for ind, command in enumerate(cli_config.core_commands):
        if command.name == "db":
            db_command_ind = ind
            break

    db_command = cli_config.core_commands[db_command_ind]
    if any(sc.name == "trim" for sc in db_command.subcommands):
        # Avoid adding "trim" subcommand more than once.
        return

    # Add "trim" subcommand to "airflow db" command.
    cli_config.core_commands[db_command_ind] = db_command._replace(
        subcommands=db_command.subcommands
        + (
            cli_config.ActionCommand(
                name="trim",
                help="(Cloud Composer internal) Clean up database in small transactions",
                func=cli_config.lazy_load_command(
                    "airflow.composer.patches.database_retention.db_command.trim"
                ),
                args=(
                    ARG_DB_TRIM_RETENTION_DAYS,
                    ARG_DB_TRIM_COMPOSER_INTERNAL,
                    ARG_DB_TRIM_BATCH_SIZE,
                    ARG_DB_TRIM_SLEEP,
                ),
            ),
        )
    )
