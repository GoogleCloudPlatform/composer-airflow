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

import inspect

from uvicorn.supervisors.multiprocess import Process


def patch():
    patch_process_is_alive()


def patch_process_is_alive():
    """
    Patch default value of the `timeout` parameter of Process.is_alive method.

    Starting multiple uvicorn workers (api-server) at the same time in airflow-webserver creates high CPU
    consumption, which results in CPU starvation that causes uvicorn worker alive checks to fail due to not
    enough timeout value for response (especially noticeable in case of small-CPU airflow-webserver,
    e.g. 0.5).

    Here, we bump default value (which is used by parent process) of the `timeout` parameter to address
    described above issue.
    """
    is_alive_signature = inspect.signature(Process.is_alive)

    # Verify that signature of Process.is_alive is as expected - it has two parameters (self and timeout), and
    # `timeout` parameter has 5 as default value.
    if list(is_alive_signature.parameters.keys()) != ["self", "timeout"]:
        raise ValueError("Unexpected list of parameters in Process.is_alive method")
    if Process.is_alive.__defaults__ != (5,):
        raise ValueError("Unexpected parameter default values in Process.is_alive method")

    # Modify default value of `timeout` parameter.
    Process.is_alive.__defaults__ = (60,)
