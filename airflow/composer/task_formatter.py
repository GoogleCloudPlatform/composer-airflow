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
    formatted_task_info = _LOG_SEPARATOR + json.dumps(workflow_info)
    setattr(record, _WORKFLOW_INFO_RECORD_KEY, formatted_task_info)


class TaskFormatter(logging.Formatter):
    """Formatter that appends additional task metadata for Cloud Logging."""

    def __init__(self, fmt: str = None, *args, **kwargs):
        fmt_task = (fmt or "%(message)s") + f"%({_WORKFLOW_INFO_RECORD_KEY})s"
        super().__init__(fmt_task, *args, **kwargs)

    def format(self, record: logging.LogRecord):
        formatted_message = super().format(record)
        if hasattr(record, _WORKFLOW_INFO_RECORD_KEY) and (record.stack_info or record.exc_info):
            # Exception or stack info are emmited as separate log entries
            # from the perspective of Cloud Logging and have to be annotated
            # with metadata separately.
            formatted_message += getattr(record, _WORKFLOW_INFO_RECORD_KEY)
        return formatted_message
