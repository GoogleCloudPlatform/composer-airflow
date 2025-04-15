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
from structlog.contextvars import get_contextvars

from airflow.composer.patches.logging.monkey_patching.airflow_sdk_execution_time_supervisor import patch
from airflow.sdk.execution_time import supervisor


class TestAirflowSdkExecutionTimeSupervisor:
    @mock.patch("airflow.sdk.execution_time.supervisor.supervise", autospec=True)
    def test_patch(self, supervise_mock):
        def supervise_mock_side_effect(ti, bundle_info, dag_rel_path, token):
            supervise_mock._context_vars = get_contextvars()

        supervise_mock.side_effect = supervise_mock_side_effect
        patch()

        supervisor.supervise(
            ti=mock.Mock(dag_id="dag-id", task_id="task-id", run_id="run-id", map_index=-1, try_number=1),
            bundle_info=mock.Mock(),
            dag_rel_path=mock.Mock(),
            token=mock.Mock(),
        )

        assert supervise_mock._context_vars == {
            "composer_ti_info": {
                "workflow": "dag-id",
                "task-id": "task-id",
                "run-id": "run-id",
                "map-index": "-1",
                "try-number": "1",
            }
        }

    @pytest.mark.parametrize(
        "os_environ_patch, expected_result",
        [
            ({"AIRFLOW_IS_K8S_EXECUTOR_POD": "True"}, {"subprocess_logs_to_stdout": False}),
            ({}, {}),
        ],
    )
    @mock.patch("airflow.sdk.execution_time.supervisor.supervise", autospec=True)
    def test_patch_subprocess_logs_to_stdout(self, supervise_mock, os_environ_patch, expected_result):
        def supervise_mock_side_effect(ti, bundle_info, dag_rel_path, token, **extra_kwargs):
            return extra_kwargs

        supervise_mock.side_effect = supervise_mock_side_effect
        patch()

        with mock.patch.dict("os.environ", os_environ_patch):
            actual_result = supervisor.supervise(
                ti=mock.Mock(dag_id="dag-id", task_id="task-id", run_id="run-id", map_index=-1, try_number=1),
                bundle_info=mock.Mock(),
                dag_rel_path=mock.Mock(),
                token=mock.Mock(),
            )

        assert actual_result == expected_result
