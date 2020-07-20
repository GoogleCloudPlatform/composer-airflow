# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from typing import Optional

from airflow import version


def get_docs_url(page: Optional[str] = None) -> str:
    """Prepare link to Airflow documentation."""
    if "dev" in version.version:
        result = "https://airflow.readthedocs.io/en/latest/"
    else:
        result = 'https://airflow.apache.org/docs/{}/'.format(version.version)
    if page:
        result = result + page
    return result
