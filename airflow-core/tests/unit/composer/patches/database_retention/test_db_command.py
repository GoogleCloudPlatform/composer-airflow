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

from unittest import mock

import pytest

from airflow.composer.patches.database_retention.monkey_patching.airflow_cli_cli_config import patch


class TestDbCommand:
    def setup_method(self):
        patch()

        from airflow.cli import cli_parser

        with mock.patch.dict("os.environ", {"COMPOSER_ENVIRONMENT_SIZE": "ENVIRONMENT_SIZE_SMALL"}):
            from airflow.composer.patches.database_retention.db_command import trim

        self.trim = trim
        self.parser = cli_parser.get_parser()

    @pytest.mark.parametrize(
        "command, expected_args, expected_kwargs",
        [
            (
                # Default.
                ["db", "trim", "--acknowledge-composer-internal", "--retention-days", "30"],
                (30,),
                {"batch_size": 1000, "sleep_between_batches_seconds": 0.5},
            ),
            (
                # --retention-batch-size/--retention-sleep args.
                [
                    "db",
                    "trim",
                    "--acknowledge-composer-internal",
                    "--retention-days",
                    "30",
                    "--retention-batch-size",
                    "1234",
                    "--retention-sleep",
                    "0.23",
                ],
                (30,),
                {"batch_size": 1234, "sleep_between_batches_seconds": 0.23},
            ),
            (
                # --retention-batch-size/--retention-sleep capped with minimal values.
                [
                    "db",
                    "trim",
                    "--acknowledge-composer-internal",
                    "--retention-days",
                    "30",
                    "--retention-batch-size",
                    "1",
                    "--retention-sleep",
                    "0.05",
                ],
                (30,),
                {"batch_size": 1000, "sleep_between_batches_seconds": 0.1},
            ),
            (
                # --retention-batch-size/--retention-sleep capped with maximal values.
                [
                    "db",
                    "trim",
                    "--acknowledge-composer-internal",
                    "--retention-days",
                    "30",
                    "--retention-batch-size",
                    "999999",
                    "--retention-sleep",
                    "1.0",
                ],
                (30,),
                {"batch_size": 100000, "sleep_between_batches_seconds": 0.5},
            ),
        ],
    )
    @mock.patch("airflow.composer.patches.database_retention.db_command.execute_trim", autospec=True)
    def test_trim(self, execute_trim_mock, command, expected_args, expected_kwargs):
        self.trim(self.parser.parse_args(command))

        execute_trim_mock.assert_called_once_with(*expected_args, **expected_kwargs)

    @mock.patch("airflow.composer.patches.database_retention.db_command.execute_trim", autospec=True)
    @mock.patch(
        "airflow.composer.patches.database_retention.db_command.COMPOSER_ENVIRONMENT_SIZE",
        "ENVIRONMENT_SIZE_EXTRA_LARGE",
    )
    def test_trim_xl_env(self, execute_trim_mock):
        self.trim(
            self.parser.parse_args(
                ["db", "trim", "--acknowledge-composer-internal", "--retention-days", "30"]
            )
        )

        execute_trim_mock.assert_called_once_with(30, batch_size=15000, sleep_between_batches_seconds=0.2)

    def test_trim_retention_days_out_of_range(self):
        with pytest.raises(ValueError) as exc:
            self.trim(
                self.parser.parse_args(
                    ["db", "trim", "--acknowledge-composer-internal", "--retention-days", "1"]
                )
            )

        assert str(exc.value) == "Retention horizon must be in range(30, 730)"

    def test_trim_no_acknowledge_composer_internal_flag(self):
        with pytest.raises(AssertionError) as exc:
            self.trim(self.parser.parse_args(["db", "trim", "--retention-days", "30"]))

        assert (
            str(exc.value)
            == "`airflow db trim` is an internal Cloud Composer command. Specify the --acknowledge-composer-internal flag to suppress this error."
        )
