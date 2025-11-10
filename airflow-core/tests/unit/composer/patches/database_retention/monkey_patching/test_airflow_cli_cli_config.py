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
from airflow.composer.patches.database_retention.monkey_patching.airflow_cli_cli_config import patch


class TestAirflowCliCliConfig:
    def test_patch(self):
        def _get_db_trim_command():
            for command in cli_config.core_commands:
                if command.name == "db":
                    for subcommand in command.subcommands:
                        if subcommand.name == "trim":
                            return subcommand

        assert not _get_db_trim_command()

        patch()

        db_trim_command = _get_db_trim_command()
        assert db_trim_command
        assert db_trim_command.help == "(Cloud Composer internal) Clean up database in small transactions"
        assert db_trim_command.func.__name__ == "trim"
        assert [a.flags for a in db_trim_command.args] == [
            ("--retention-days",),
            ("--acknowledge-composer-internal",),
            ("--retention-batch-size",),
            ("--retention-sleep",),
        ]
