#
# Copyright 2020 Google LLC
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
import hashlib
import os
import uuid
from typing import List

LOCATION_PATH = "projects/{}/locations/{}".format(
    os.environ.get("GCP_PROJECT"), os.environ.get("COMPOSER_LOCATION")
)


def generate_uuid_from_string(s: str) -> str:
    """Returns string representation of UUID generated from given string."""
    md5_hash = hashlib.md5()
    md5_hash.update(s.encode("utf-8"))
    return str(uuid.UUID(md5_hash.hexdigest()))


def get_process_id(environment_name: str, dag_id: str, task_id: str) -> str:
    """Returns lineage process id generated from given parameters.

    Airflow task corresponds to Data Lineage Process, therefore Composer environment name,
    DAG id and task id uniquely identify Process.

    Returns:
        Suffix for full Process name "projects/{project}/locations/{location}/processes/{process}".
    """
    uuid1 = generate_uuid_from_string(environment_name)
    uuid2 = generate_uuid_from_string(dag_id)
    uuid3 = generate_uuid_from_string(task_id)

    return generate_uuid_from_string(uuid1 + uuid2 + uuid3)


def get_run_id(task_instance_run_id: str) -> str:
    """Returns lineage run id generated from given parameter.

    Airflow task_instance corresponds to Data Lineage Run, therefore task_instance.run_id
    uniquely identifies Run.

    Returns:
        Suffix for full Run name "projects/{project}/locations/{location}/processes/{process}/runs/{run}".
    """
    return generate_uuid_from_string(task_instance_run_id)


def exclude_outlet(inlets: List[object], outlet: object) -> List[object]:
    """Excludes outlet from the given list of inlets.

    Args:
        inlets: List of inlets.
        outlet: Outlet that must be excluded from the inlets list.

    Returns:
        Copy of the given list of inlets without given outlet.
    """
    # TODO: fix inlets containing outlet and remove this temporary workaround. We have this workaround
    #  for now as it is rather an edge case when inlets containing outlet.
    return [_inlet for _inlet in inlets if _inlet != outlet]
