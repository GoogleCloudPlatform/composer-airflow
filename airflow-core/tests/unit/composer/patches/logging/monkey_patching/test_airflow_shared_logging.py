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
import sys
from unittest import mock

import pytest

from airflow._shared import logging as airflow_shared_logging
from airflow.composer.patches.logging.monkey_patching.airflow_shared_logging import (
    _patch_stdlib_root_logger,
    patch,
)


class TestAirflowSharedLogging:
    @mock.patch("airflow._shared.logging.configure_logging", return_value="mocked")
    @mock.patch(
        "airflow.composer.patches.logging.monkey_patching.airflow_shared_logging.filter_warnings",
        autospec=True,
    )
    @mock.patch(
        "airflow.composer.patches.logging.monkey_patching.airflow_shared_logging._patch_stdlib_root_logger",
        autospec=True,
    )
    def test_patch(self, patch_stdlib_root_logger_mock, filter_warnings_mock, configure_logging_mock):
        patch()

        res = airflow_shared_logging.configure_logging()

        assert res == "mocked"
        filter_warnings_mock.assert_called_once_with()
        patch_stdlib_root_logger_mock.assert_called_once_with()

    @mock.patch(
        "airflow.composer.patches.logging.monkey_patching.airflow_shared_logging.logging.root.handlers",
        [mock.Mock(), mock.Mock(), mock.Mock()],
    )
    def test_patch_stdlib_root_logger_mock(self):
        # Since name on mock attribute can't be set at creation time, we define it (and stream) here.
        logging.root.handlers[0].name = "handler1"
        logging.root.handlers[0].stream = sys.stderr
        logging.root.handlers[1].name = "default"
        logging.root.handlers[1].stream = sys.stderr
        logging.root.handlers[2].name = "handler2"
        logging.root.handlers[2].stream = sys.stderr

        _patch_stdlib_root_logger()

        assert logging.root.handlers[0].stream == sys.stderr
        assert logging.root.handlers[1].stream == sys.stdout
        assert logging.root.handlers[2].stream == sys.stderr

    @mock.patch(
        "airflow.composer.patches.logging.monkey_patching.airflow_shared_logging.logging.root.handlers",
        [mock.Mock()],
    )
    def test_patch_stdlib_root_logger_mock_no_default_handler(self):
        # Since name on mock attribute can't be set at creation time, we define it (and stream) here.
        logging.root.handlers[0].name = "handler1"
        logging.root.handlers[0].stream = sys.stderr

        with pytest.raises(ValueError) as exc:
            _patch_stdlib_root_logger()

        assert str(exc.value) == "'default' handler is not found for root logger"
