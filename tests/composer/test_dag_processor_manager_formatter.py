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
from __future__ import annotations

import importlib
import io
import logging
import unittest
from unittest.mock import patch

from airflow.config_templates import airflow_local_settings
from airflow.logging_config import configure_logging


class TestDagProcessorManagerFormatter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with patch.dict("os.environ", {"CONFIG_PROCESSOR_MANAGER_LOGGER": "True"}):
            importlib.reload(airflow_local_settings)
            configure_logging()

    def add_stream_handler_to_logger(self, logger: logging.Logger) -> io.StringIO:
        stream = io.StringIO()
        new_handler = logging.StreamHandler(stream)
        new_handler.setFormatter(logger.handlers[0].formatter)
        logger.addHandler(new_handler)
        return stream

    def test_single_line(self):
        logger = logging.getLogger("airflow.processor_manager")
        with self.add_stream_handler_to_logger(logger) as stream:
            message = "Test message"
            logger.warning(message)
            self.assertRegex(stream.getvalue(), "DAG_PROCESSOR_MANAGER_LOG:.*Test message")

    def test_multi_line(self):
        logger = logging.getLogger("airflow.processor_manager")
        with self.add_stream_handler_to_logger(logger) as stream:
            message = "\n".join(["line-1", "line-2", "last-line"])
            logger.warning(message)
            self.assertRegex(stream.getvalue(), "DAG_PROCESSOR_MANAGER_LOG:.*line-1")
            self.assertIn("DAG_PROCESSOR_MANAGER_LOG:line-2", stream.getvalue())
            self.assertIn("DAG_PROCESSOR_MANAGER_LOG:last-line", stream.getvalue())
