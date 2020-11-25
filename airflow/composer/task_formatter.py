#
# Copyright 2022 Google LLC
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
"""A formatter that appends metadata for Cloud Logging."""
import json
import logging
from typing import Dict

_LOG_SEPARATOR = "@-@"
_WORKFLOW_INFO_RECORD_KEY = "workflow_info"
_EXTRA_WORKFLOW_INFO_RECORD_KEY = "extra_workflow_info"
# Max size of Cloud Logging log entry is ~256K ~ 65000 4-byte chars ~260000 1-byte chars.
# Given that log entry also includes label and 4096 characters is a full screen
# of text, splitting them at 4096 should be fine.
_LOG_LINE_SPLIT_LENGTH = 4096


def strip_separator_from_log(log: str):
    """Remove workflow metadata from logs."""
    lines = []
    for line in log.split("\n"):
        idx = line.rfind(_LOG_SEPARATOR)
        if idx >= 0:
            line = line[:idx]
        lines.append(line)
    return "\n".join(lines)


def set_task_log_info(record: logging.LogRecord, workflow_info: Dict[str, str]) -> str:
    """Set formatted task info field in the log record."""
    extra_workflow_info = getattr(record, _EXTRA_WORKFLOW_INFO_RECORD_KEY, {})

    full_workflow_info = {}
    full_workflow_info.update(workflow_info)
    full_workflow_info.update(extra_workflow_info)

    formatted_task_info = _LOG_SEPARATOR + json.dumps(full_workflow_info)
    setattr(record, _WORKFLOW_INFO_RECORD_KEY, formatted_task_info)


class TaskFormatter(logging.Formatter):
    """Formatter that appends additional task metadata for Cloud Logging."""

    def format(self, record: logging.LogRecord):
        formatted_message = super().format(record)
        lines_to_escape = [
            formatted_message[i : i + _LOG_LINE_SPLIT_LENGTH]
            for i in range(0, len(formatted_message), _LOG_LINE_SPLIT_LENGTH)
        ]
        # New lines are mostly translated into a new log entry in Cloud Logging.
        # But for some patterns that do not apply as GKE logging processor can
        # combine some of the lines, for ex. Python error traces.
        # To keep logging consitent, escape all new line characters and
        # translate them back in composer-fluentd. This way log entries will be
        # consistent even if they are multi-line and/or have exception traces.
        escaped_lines = map(
            lambda line: line.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r"),
            lines_to_escape,
        )
        if hasattr(record, _WORKFLOW_INFO_RECORD_KEY):
            workflow_annotation = getattr(record, _WORKFLOW_INFO_RECORD_KEY)
            escaped_lines = map(lambda line: line + workflow_annotation, escaped_lines)
        return "\n".join(escaped_lines)
