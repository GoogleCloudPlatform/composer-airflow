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
from unittest.mock import Mock, patch

import pytest
from parameterized import parameterized

from airflow.providers.tableau.sensors.tableau import (
    TableauJobFailedException,
    TableauJobFinishCode,
    TableauJobStatusSensor,
)


class TestTableauJobStatusSensor(unittest.TestCase):
    """
    Test Class for JobStatusSensor
    """

    def setUp(self):
        self.kwargs = {"job_id": "job_2", "site_id": "test_site", "task_id": "task", "dag": None}

    @patch("airflow.providers.tableau.sensors.tableau.TableauHook")
    def test_poke(self, mock_tableau_hook):
        """
        Test poke
        """
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        mock_tableau_hook.get_job_status.return_value = TableauJobFinishCode.SUCCESS
        sensor = TableauJobStatusSensor(**self.kwargs)

        job_finished = sensor.poke(context={})

        assert job_finished
        mock_tableau_hook.get_job_status.assert_called_once_with(job_id=sensor.job_id)

    @parameterized.expand([(TableauJobFinishCode.ERROR,), (TableauJobFinishCode.CANCELED,)])
    @patch("airflow.providers.tableau.sensors.tableau.TableauHook")
    def test_poke_failed(self, finish_code, mock_tableau_hook):
        """
        Test poke failed
        """
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        mock_tableau_hook.get_job_status.return_value = finish_code
        sensor = TableauJobStatusSensor(**self.kwargs)

        with pytest.raises(TableauJobFailedException):
            sensor.poke({})
        mock_tableau_hook.get_job_status.assert_called_once_with(job_id=sensor.job_id)
