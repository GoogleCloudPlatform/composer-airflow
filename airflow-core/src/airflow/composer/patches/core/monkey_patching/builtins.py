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

import builtins
import functools


def patch():
    # Builtin exit method closes stdin and raises SystemExit exception. Here, we patch it to just raise
    # SystemExit exception (without closing stdin). See https://github.com/apache/airflow/issues/55783 for
    # details.
    # TODO: remove this monkey patching in the Composer Airflow versions containing
    #  https://github.com/apache/airflow/pull/55786.
    builtins.exit = _composer_builtins_exit(builtins.exit)


def _composer_builtins_exit(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        raise SystemExit(*args, **kwargs)

    return wrapper
