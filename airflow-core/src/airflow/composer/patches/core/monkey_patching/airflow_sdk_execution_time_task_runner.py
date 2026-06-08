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
import time

from airflow.configuration import conf
from airflow.sdk.execution_time import task_runner

logger = logging.getLogger(__name__)


def patch():
    task_runner.parse = _composer_task_runner_parse(task_runner.parse)


def _composer_task_runner_parse(f):
    """
    Handle gracefully DAG parsing failure during task execution.

    This patch will assure that DAG parsing is retried if it fails. This is need to gracefully handle scenario
    when DAG file(s) are not yet synced to worker (resulting in DAG parsing failure), while they were already
    synced to DAG processor and scheduler, and DAG was already scheduled for execution.

    DAG parsing will be done in a cycle until it succeeds or times out (time out controlled by
    [core]wait_dag_not_found_timeout Airflow configuration property).
    """

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        wait_dag_not_found_timeout = conf.getint("core", "wait_dag_not_found_timeout", fallback=0)
        start_time = time.time()

        while True:
            time_passed_before_parse = time.time() - start_time

            try:
                result = f(*args, **kwargs)
            except SystemExit:
                # SystemExit exception is raised ("exit" method is called) when DAG or task was not found
                # after parsing DAG file(s).
                # It is expected to happen in case DAG file(s) are not yet synced to worker, while they were
                # already synced to DAG processor and scheduler, and DAG was already scheduled for execution.
                # In this case, we catch exception and will retry in the next iteration.
                pass
            else:
                # If there is no exception, then break the loop and return result.
                break

            if time_passed_before_parse > wait_dag_not_found_timeout:
                raise SystemExit(1)

            sleep_time = 5
            logger.warning(
                "DAG or task is not found in loaded DAG bag. Retrying after %s seconds.", sleep_time
            )
            time.sleep(sleep_time)

        return result

    return wrapper
