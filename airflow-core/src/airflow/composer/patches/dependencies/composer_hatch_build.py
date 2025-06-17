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
"""Module with patches for Airflow hatch_build.py."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from hatchling.builders.config import BuilderConfig
    from hatchling.builders.hooks.plugin.interface import BuildHookInterface

COMPOSER_DEPENDENCIES = [
    "apache-airflow-providers-apache-beam",
    "apache-airflow-providers-celery",
    "apache-airflow-providers-cncf-kubernetes",
    "apache-airflow-providers-dbt-cloud",
    "apache-airflow-providers-fab",
    "apache-airflow-providers-google",
    "apache-airflow-providers-hashicorp",
    "apache-airflow-providers-http",
    "apache-airflow-providers-mysql",
    "apache-airflow-providers-openlineage",
    "apache-airflow-providers-postgres",
    "apache-airflow-providers-redis",
    "apache-airflow-providers-sendgrid",
    "apache-airflow-providers-sqlite",
    "apache-airflow-providers-ssh",
    "apache-airflow-providers-standard",
    "apache-airflow-task-sdk",
    "aiodebug",
    "aiohttp>=3.8.5",  # security vulnerabilities in lower versions.
    "crcmod<2.0",
    "cryptography",
    "dbt-bigquery",
    "dbt-core",
    "firebase-admin",
    # TODO: remove once https://github.com/apache/airflow/issues/36897 is closed
    "Flask-Session<0.6.0",
    "flower>=2.0.0",  # security vulnerabilities in lower versions.
    "gcsfs",
    "google-apitools",
    "google-cloud-aiplatform[evaluation]",
    "google-cloud-asset",
    "google-cloud-datastore",
    "google-cloud-documentai",
    "google-cloud-filestore",
    "google-cloud-firestore",
    "google-cloud-pubsublite<1.0.0",
    "keyrings.google-artifactregistry-auth",
    "pip==23.2.1",
    "pyOpenSSL",
    "pipdeptree",
    "pygments>2.15.0",  # security vulnerabilities in lower versions.
    "pytest!=8.4.0",  # only as a constraint, https://github.com/pytest-dev/pytest/issues/13477.
    "sqlalchemy-spanner!=1.12.0",  # only as a constraint, https://github.com/googleapis/python-spanner-sqlalchemy/issues/682.
    "sqllineage",
    "sqlparse",
    "tensorflow",
    "virtualenv>=20.24.0",  # security vulnerabilities in lower versions.
    "websockets",
    "werkzeug>=2.2.3",  # security vulnerabilities in lower versions.
]

# Composer dependencies that are not from pypi.org (from other repositories).
COMPOSER_NON_PYPI_ORG_DEPENDENCIES = [
    "google-cloud-datacatalog-lineage-producer-client",
]

# Airflow extras that are Composer dependencies.
COMPOSER_EXTRAS_DEPENDENCIES = [
    "statsd",
]


def build_hook_initialize(build_hook: BuildHookInterface[BuilderConfig], build_data: dict[str, Any]) -> None:
    """Perform Composer-specific modifications in the build hook."""
    build_data["dependencies"].extend(COMPOSER_DEPENDENCIES)
    build_data["dependencies"].extend(COMPOSER_NON_PYPI_ORG_DEPENDENCIES)

    for composer_extra_dependency in COMPOSER_EXTRAS_DEPENDENCIES:
        build_data["dependencies"].extend(
            build_hook.metadata.core._optional_dependencies[composer_extra_dependency]
        )
