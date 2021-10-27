#
# Copyright 2021 Google LLC
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
import os
import unittest

from airflow.composer.kubernetes.executor import POD_TEMPLATE_FILE, refresh_pod_template_file


class TestExecutor(unittest.TestCase):
    def test_refresh_pod_template_file(self):
        if os.path.exists(POD_TEMPLATE_FILE):
            os.remove(POD_TEMPLATE_FILE)
        assert os.path.exists(POD_TEMPLATE_FILE) is False

        refresh_pod_template_file()

        assert os.path.exists(POD_TEMPLATE_FILE) is True
        with open(POD_TEMPLATE_FILE) as f:
            assert "dummy-name-dont-delete" in f.read()
