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

import logging
import re2

from airflow.composer.task_formatter import _EXTRA_WORKFLOW_INFO_RECORD_KEY

log: logging.Logger | logging.LoggerAdapter = logging.getLogger(__name__)
log = logging.LoggerAdapter(log, {_EXTRA_WORKFLOW_INFO_RECORD_KEY: {"log-type": "data_lineage"}})

def sanitize_display_name(display_name: str) -> str:
    """Sanitizes display_name for Process and Run.

    See Data Lineage API spec for supported characters.
    """
    return re2.sub(r"[^A-Za-z0-9 _\-:&.]", "", display_name)[:200]


def _clean_schema_and_producer(value):
    if type(value) != dict:
        return value

    clean_dict = value.copy()
    del clean_dict['_producer']
    del clean_dict['_schemaURL']
    return clean_dict


def _traverse_copy(original_dict, fields):
    """Returns a (possibly shallow) copy of original_dict, containing only those given fields.
    If we get to a list in original_dict (for example, inputs and outputs) we recursively copy each item.
    We also clean the _producer and _schemaURL fields.
    """
    copy_dict = {}
    for field, new_fields in fields.items():
        if field not in original_dict:
            continue

        if new_fields == True:
            copy_dict[field] = _clean_schema_and_producer(original_dict[field])
        elif type(original_dict[field]) == list:
            new_list = []
            for item in original_dict[field]:
                new_list.append(_traverse_copy(item, new_fields))
            copy_dict[field] = new_list
        else:
            copy_dict[field] = _traverse_copy(original_dict[field], new_fields)

    return copy_dict


def get_redacted_event(event: dict) -> dict:
    """Log redacted version of the OL event.

    We make sure to log only fields that do not pose any privacy or security concerns and are useful
    for users, as full version is too verbose.
    """

    FIELDS_TO_LOG = {
        "eventTime": True,
        "eventType": True,
        "inputs": {
            "name": True,
            "namespace": True,
        },
        "outputs": {
            "name": True,
            "namespace": True,
        },
        "job": {
            "facets": {
                "gcp_composer_job": True,
                "gcp_lineage": True,
                "jobType": {
                    "jobType": True,
                },
            },
            "name": True,
            "namespace": True,
        },
        "run": {
            "facets":{
                "gcp_composer_run": True
            }
        },
    }

    return _traverse_copy(event, FIELDS_TO_LOG)
