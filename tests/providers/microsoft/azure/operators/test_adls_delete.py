#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import unittest
from unittest import mock

from airflow.providers.microsoft.azure.operators.adls import ADLSDeleteOperator

TASK_ID = "test-adls-list-operator"
TEST_PATH = "test"


class TestAzureDataLakeStorageDeleteOperator(unittest.TestCase):
    @mock.patch("airflow.providers.microsoft.azure.operators.adls.AzureDataLakeHook")
    def test_execute(self, mock_hook):

        operator = ADLSDeleteOperator(task_id=TASK_ID, path=TEST_PATH)

        operator.execute(None)
        mock_hook.return_value.remove.assert_called_once_with(
            path=TEST_PATH, recursive=False, ignore_not_found=True
        )
