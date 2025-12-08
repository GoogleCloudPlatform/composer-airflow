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

import functools
import logging
import sys

from airflow._shared import logging as airflow_shared_logging
from airflow.composer.patches.logging.logs_filtering import filter_warnings


def patch():
    airflow_shared_logging.configure_logging = _composer_airflow_shared_logging_configure_logging(
        airflow_shared_logging.configure_logging
    )


def _composer_airflow_shared_logging_configure_logging(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        res = f(*args, **kwargs)

        filter_warnings()
        _patch_stdlib_root_logger()

        return res

    return wrapper


def _patch_stdlib_root_logger():
    # Find 'default' handler of root logger and change its stream from sys.stderr (default) to sys.stdout.
    # In Composer, it is essential that all regular logs (not errors/exceptions/tracebacks) will go to stdout,
    # so that if for any reason severity will be missing in the log message (e.g. [INFO]), log message will
    # get INFO severity by default.
    for h in logging.root.handlers:
        if h.name == "default":
            h.stream = sys.stdout
            break
    else:
        raise ValueError("'default' handler is not found for root logger")
