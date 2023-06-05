#
# Copyright 2023 Google LLC
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
"""Helper for replacing the handlers of the root logger"""

from __future__ import annotations

import logging
from contextlib import redirect_stderr, redirect_stdout
from typing import Callable

from airflow.utils.log.logging_mixin import StreamLogWriter


class _SubstituteRootLoggerHandlers:
    def __init__(self, new_logger: logging.Logger):
        self._new_logger: logging.Logger = new_logger
        self._old_handlers = None

    def __enter__(self):
        self._old_handlers = logging.root.handlers.copy()
        logging.root.handlers.clear()

        processor_manager_logger = self._new_logger
        for handler in processor_manager_logger.handlers:
            logging.root.addHandler(handler)

        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        logging.root.handlers.clear()
        for handler in self._old_handlers:
            logging.root.addHandler(handler)


def handle_callback(callback_function: Callable, new_logger: logging.Logger, *args):
    processor_manager_logger = new_logger
    with redirect_stdout(StreamLogWriter(processor_manager_logger, logging.INFO)), redirect_stderr(
        StreamLogWriter(processor_manager_logger, logging.ERROR)
    ), _SubstituteRootLoggerHandlers(new_logger):
        callback_function(*args)
