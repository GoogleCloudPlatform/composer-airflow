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

import json
import logging
from typing import TYPE_CHECKING

from structlog import get_config
from structlog.processors import CallsiteParameter, CallsiteParameterAdder

if TYPE_CHECKING:
    from airflow.executors.workloads import TaskInstance

# Separator between actual log message and json annotation with Composer labels. This separator will be
# recognised and parsed by fluentd.
_LOG_SEPARATOR = "@-@"
# Max size of Cloud Logging log entry is ~256K ~ 65000 4-byte chars ~260000 1-byte chars.
# Given that log entry also includes labels and 4096 characters is a full screen
# of text, splitting them at 4096 should be fine.
_LOG_LINE_SPLIT_LENGTH = 4096


def get_task_logs_contextvars(ti: TaskInstance):
    """Return task logs context variables that will be used in supervisor_log_processor."""
    return {
        "composer_ti_info": {
            "workflow": ti.dag_id,
            "task-id": ti.task_id,
            "run-id": ti.run_id,
            "map-index": str(ti.map_index),
            "try-number": str(ti.try_number),
        }
    }


def patch_supervisor_log_processors():
    """
    Patch supervisor log processors.

    - add CallsiteParameterAdder processor
    - replace last processor of supervisor logs with supervisor_log_processor
    """
    processors = get_config()["processors"]

    # Remove the last processor which is rendering the message.
    processors.pop()

    # Add CallsiteParameterAdder processor and processor which will render message with custom format.
    processors.append(
        CallsiteParameterAdder(
            [
                CallsiteParameter.FILENAME,
                CallsiteParameter.FUNC_NAME,
                CallsiteParameter.LINENO,
            ]
        )
    )
    processors.append(supervisor_log_processor)


def supervisor_log_processor(logger, method_name, event_dict):
    """Render the log message with custom format and Composer labels added as a json annotation."""
    message = event_dict["event"]

    # If present, append exception to the log message.
    if error_detail := event_dict.get("error_detail"):
        message += "\n" + error_detail

    # Split message into lines with appropriate length.
    if message:
        lines_to_format = [
            message[i : i + _LOG_LINE_SPLIT_LENGTH] for i in range(0, len(message), _LOG_LINE_SPLIT_LENGTH)
        ]
    else:
        lines_to_format = [""]

    # Format lines as "[2025-03-17 12:40:03.007123] {subprocess.py:93} INFO - message", parseable by fluentd.
    formatted_lines = map(
        lambda line: (
            f"[{event_dict['timestamp']}] {{{event_dict['filename']}:{event_dict['lineno']}}} "
            f"{event_dict['level'].upper()} - {line}"
        ),
        lines_to_format,
    )

    # New lines are mostly translated into new log entries in Cloud Logging.
    # But for some patterns this does not apply as GKE logging processor can
    # combine some of the lines, for ex. Python error traces.
    # To keep logging consistent, escape all new line characters and
    # translate them back in composer-fluentd. This way log entries will be
    # consistent even if they are multi-line and/or have exception traces.
    escaped_lines = map(
        lambda line: line.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r"),
        formatted_lines,
    )

    # Annotate lines with Composer log labels.
    annotation_dict = {"function": event_dict["func_name"]}
    annotation_dict.update(event_dict.get("composer_ti_info", {}))
    annotation_dict.update(event_dict.get("composer_extra_info", {}))
    annotation = _LOG_SEPARATOR + json.dumps(annotation_dict)
    annotated_lines = map(
        lambda line: line + annotation,
        escaped_lines,
    )

    return "\n".join(annotated_lines)


def patch_supervisor_stdlib_logging_configuration():
    """Set custom format (same as for structlog logs) for stdlib logs."""
    formatter = logging.Formatter("[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s")
    logging.root.handlers[0].setFormatter(formatter)
