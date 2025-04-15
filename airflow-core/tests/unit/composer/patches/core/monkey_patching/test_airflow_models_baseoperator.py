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

from airflow.composer.patches.core.monkey_patching.airflow_models_baseoperator import patch
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models.baseoperator import BaseOperator

from tests_common.test_utils.config import conf_vars


class TestAirflowModelsBaseOperator:
    @classmethod
    def setup_class(cls):
        patch()

    def test_patch(self):
        task = BaseOperator(task_id="test_task")

        with pytest.raises(
            AirflowException, match="This Composer environment does not have Airflow triggerer running."
        ):
            task.defer(trigger=mock.Mock(), method_name="test")

    @conf_vars({("composer_internal", "enable_triggerer"): "True"})
    def test_patch_triggerer_running(self):
        task = BaseOperator(task_id="test_task")

        with pytest.raises(TaskDeferred):
            task.defer(trigger=mock.Mock(), method_name="test")
