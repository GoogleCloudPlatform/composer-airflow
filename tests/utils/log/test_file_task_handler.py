#
# Copyright 2020 Google LLC
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
import logging
from datetime import datetime
from unittest import mock

from airflow.utils.log.file_task_handler import WorkflowContextProcessor


class TestFileTaskHandler:
    def test_workflow_context_processor(self):
        workflow_context_processor = WorkflowContextProcessor()
        workflow_context_processor.set_context(mock.Mock(
            dag_id="test_dag_id",
            task_id="test_task_id",
            execution_date=datetime(2019, 1, 2),
            map_index=3,
            try_number=2,
        ))

        record = logging.makeLogRecord({})
        workflow_context_processor.add_context_to_record(record)

        assert record.workflow_info == (
            '@-@{"workflow": "test_dag_id", "task-id": "test_task_id", "execution-date": "2019-01-02T00:00:00", '
            '"map-index": "3", "try-number": "2"}'
        )
