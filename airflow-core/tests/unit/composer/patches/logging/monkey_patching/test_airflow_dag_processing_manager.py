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

import structlog

from airflow.composer.patches.logging.monkey_patching.airflow_dag_processing_manager import patch
from airflow.dag_processing.manager import DagFileProcessorManager


class TestAirflowDagProcessingManager:
    @mock.patch.object(
        DagFileProcessorManager,
        "_get_logger_for_dag_file",
        mock.Mock(return_value=(mock.Mock(), "logger_filehandle")),
    )
    def test_patch(self):
        manager = DagFileProcessorManager(max_runs=1)
        patch()

        actual_result = manager._get_logger_for_dag_file(mock.Mock())

        assert isinstance(actual_result[0]._logger, structlog.BytesLogger)
        assert actual_result[0]._logger._file.name == "/dev/null"
        assert actual_result[0]._logger._file.mode == "ab"
        assert actual_result[1] == "logger_filehandle"
