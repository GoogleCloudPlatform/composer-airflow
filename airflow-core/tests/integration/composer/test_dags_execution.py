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

import os
import shutil
import time

from integration.composer.utils import AIRFLOW_HOME

# Dag processor refresh interval is set to 5 seconds (configured in pypi-dependencies repo), so technically
# 10 seconds from the moment of Dag files being deployed should be enough for them to be parsed.
DAGS_ARE_PARSED_TIME = 10


class TestDagsExecution:
    @classmethod
    def setup_class(cls):
        # Deploy Dags to Airflow.
        current_dir = os.path.dirname(os.path.abspath(__file__))
        shutil.copytree(os.path.join(current_dir, "test_data/dags/"), os.path.join(AIRFLOW_HOME, "dags/"))

    def test_basic_dag(self):
        """This test verifies that "basic_dag" Dag is executed successfully."""
        # Allow "basic_dag" Dag to be parsed and executed, 1 second - for execution.
        time.sleep(DAGS_ARE_PARSED_TIME + 1)

        assert os.path.exists(os.path.join(AIRFLOW_HOME, "basic_dag_python_touch_file"))
        assert os.path.exists(os.path.join(AIRFLOW_HOME, "basic_dag_bash_touch_file"))

    def test_dag_with_deferrable_operator(self):
        """This test verifies that "dag_with_deferrable_operator" Dag is executed successfully."""
        # Allow "dag_with_deferrable_operator" Dag to be parsed and executed, 120 seconds - for execution.
        time.sleep(DAGS_ARE_PARSED_TIME + 120)

        assert os.path.exists(os.path.join(AIRFLOW_HOME, "dag_with_deferrable_operator_touch_file"))
