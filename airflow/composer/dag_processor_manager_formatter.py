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
"""This module stores a formatter used by DAG processor manager logger."""

from __future__ import annotations

import logging

DAG_PROCESSOR_LOG_PREFIX = "DAG_PROCESSOR_MANAGER_LOG:"


class DagProcessorManagerFormatter(logging.Formatter):
    """Formatter used to convert a DAG processor manager log record to a Composer format.

    It adds the specific prefix, which allows fluentd to send the log to the
    correct Cloud Logging logs source.
    """

    def format(self, record):
        log_text = super().format(record)
        log_text = log_text.replace("\n", "\n" + DAG_PROCESSOR_LOG_PREFIX)
        return DAG_PROCESSOR_LOG_PREFIX + log_text
