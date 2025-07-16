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

from airflow.providers.fab.www.session import AirflowDatabaseSessionInterface


def patch():
    AirflowDatabaseSessionInterface.get_expiration_time = _composer_get_expiration_time(
        AirflowDatabaseSessionInterface.get_expiration_time
    )


def _composer_get_expiration_time(f):
    @functools.wraps(f)
    def wrapper(self, app, session):
        # If _expiration_time is present then use it. _expiration_time field is set in /auth/token view.
        if session.get("_expiration_time") is not None:
            return session["_expiration_time"]

        return f(self, app, session)

    return wrapper
