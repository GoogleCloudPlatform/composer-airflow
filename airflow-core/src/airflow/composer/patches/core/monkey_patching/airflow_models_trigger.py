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

from airflow.models.trigger import Trigger


def patch():
    Trigger.rotate_fernet_key = _composer_trigger_rotate_fernet_key(Trigger.rotate_fernet_key)


def _composer_trigger_rotate_fernet_key(f):
    @functools.wraps(f)
    def wrapper(self):
        # Trigger kwargs might be stored as a plain serialized dict
        if self.encrypted_kwargs.startswith("{"):
            return

        f(self)

    return wrapper
