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

from structlog import get_config
from structlog._frames import _format_exception
from structlog.processors import ExceptionRenderer


def patch_task_runner_log_processors():
    """
    Patch task runner log processors.

    Find processor which renders exception and override format_exception method with method that renders
    exception as a string. The exception itself will be appended to the log message in the supervisor.

    Community uses ExceptionDictTransformer as a renderer, which is not suitable for Composer.
    """
    processors = get_config()["processors"]

    for processor in processors:
        if isinstance(processor, ExceptionRenderer):
            processor.format_exception = _format_exception
            break
