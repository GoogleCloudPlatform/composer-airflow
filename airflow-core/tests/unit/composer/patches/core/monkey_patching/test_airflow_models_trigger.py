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

from pendulum import DateTime, Timezone

from airflow.composer.patches.core.monkey_patching.airflow_models_trigger import patch
from airflow.models.trigger import Trigger

SERIALIZED_KWARGS = '{"__var": {"moment": {"__var": 1775119295.0, "__type": "datetime"}, "end_from_trigger": false}, "__type": "dict"}'
KWARGS = {"moment": DateTime(2026, 4, 2, 14, 32, 26, tzinfo=Timezone("UTC")), "end_from_trigger": False}


class TestAirflowModelsTrigger:
    def test_patch_unencrypted_kwargs(self):
        patch()
        trigger_row = Trigger(classpath="TriggererClass", kwargs={})
        trigger_row.encrypted_kwargs = SERIALIZED_KWARGS

        trigger_row.rotate_fernet_key()

        assert trigger_row.encrypted_kwargs == SERIALIZED_KWARGS

    def test_patch_encrypted_kwargs(self):
        patch()
        # Trigger __init__ encrypts kwargs
        trigger_row = Trigger(classpath="TriggererClass", kwargs=KWARGS)
        encrypted_kwargs_before = trigger_row.encrypted_kwargs

        trigger_row.rotate_fernet_key()

        assert trigger_row.encrypted_kwargs != encrypted_kwargs_before
        assert Trigger._decrypt_kwargs(trigger_row.encrypted_kwargs) == KWARGS
