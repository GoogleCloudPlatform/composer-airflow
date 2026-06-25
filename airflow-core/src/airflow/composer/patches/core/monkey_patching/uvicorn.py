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

import uvicorn


def patch():
    uvicorn.run = _composer_uvicorn_run(uvicorn.run)


def _composer_uvicorn_run(f):
    """
    Override default value of the `timeout_worker_healthcheck` parameter of uvicorn.run method.

    Starting multiple uvicorn workers (api-server) at the same time in airflow-webserver creates high CPU
    consumption, which results in CPU starvation that causes uvicorn worker alive checks to fail due to not
    enough timeout value for response (especially noticeable in case of small-CPU airflow-webserver,
    e.g. 1.0).

    Here, we bump default value (which is used by Airflow) of the `timeout_worker_healthcheck` parameter to
    address described above issue.

    TODO: come up with more reliable solution, e.g. passing value from Airflow code.
    """

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        kwargs["timeout_worker_healthcheck"] = 60

        return f(*args, **kwargs)

    return wrapper
