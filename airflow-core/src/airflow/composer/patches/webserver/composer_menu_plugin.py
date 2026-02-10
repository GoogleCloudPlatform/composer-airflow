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

import os
from urllib.parse import urlencode

from airflow.plugins_manager import (
    AirflowPlugin,
    AirflowPluginSource,
    register_plugin,
)

MENU_CATEGORY_NAME = "Composer"

# Links.
ENVIRONMENT_DETAILS_LINK = (
    "https://console.cloud.google.com"
    f"/composer/environments/detail/{os.environ.get('COMPOSER_LOCATION')}"
    # No url encoding needed for COMPOSER_ENVIRONMENT. From Composer docs:
    #   'The name must start with a lowercase letter followed by up to 62 lowercase letters,
    #   numbers, or hyphens, and cannot end with a hyphen.'
    f"/{os.environ.get('COMPOSER_ENVIRONMENT')}"
    "{tab}"
    "?" + urlencode({"project": os.environ.get("GCP_PROJECT")})
)
DAGS_IN_GCC_LINK = ENVIRONMENT_DETAILS_LINK.format(tab="/dags")
DAGS_IN_GCS_LINK = f"https://console.cloud.google.com/storage/browser/{os.environ.get('GCS_BUCKET')}/dags"
ENVIRONMENT_MONITORING_LINK = ENVIRONMENT_DETAILS_LINK.format(tab="/monitoring")
ENVIRONMENT_LOGS_LINK = ENVIRONMENT_DETAILS_LINK.format(tab="/logs")
COMPOSER_DOCS_LINK = "https://cloud.google.com/composer/docs"

# External views.
DAGS_GCC_EXTERNAL_VIEW = {
    "name": "DAGs in Cloud Console",
    "href": DAGS_IN_GCC_LINK,
    "destination": "nav",
    "category": MENU_CATEGORY_NAME,
}
DAGS_GCS_EXTERNAL_VIEW = {
    "name": "DAGs in Cloud Storage",
    "href": DAGS_IN_GCS_LINK,
    "destination": "nav",
    "category": MENU_CATEGORY_NAME,
}
ENV_MON_EXTERNAL_VIEW = {
    "name": "Environment Monitoring",
    "href": ENVIRONMENT_MONITORING_LINK,
    "destination": "nav",
    "category": MENU_CATEGORY_NAME,
}
ENV_LOGS_EXTERNAL_VIEW = {
    "name": "Environment Logs",
    "href": ENVIRONMENT_LOGS_LINK,
    "destination": "nav",
    "category": MENU_CATEGORY_NAME,
}
COMP_DOCS_EXTERNAL_VIEW = {
    "name": "Composer Documentation",
    "href": COMPOSER_DOCS_LINK,
    "destination": "nav",
    "category": MENU_CATEGORY_NAME,
}


def register_composer_menu_plugin():
    plugin_instance = ComposerMenuPlugin()
    plugin_instance.source = ComposerMenuPluginSource()
    register_plugin(plugin_instance)


class ComposerMenuPluginSource(AirflowPluginSource):
    """Class to define ComposerMenuPlugin source metadata."""

    def __str__(self):
        return "airflow.composer.patches.webserver.composer_menu_plugin"

    def __html__(self):
        return "airflow.composer.patches.webserver.composer_menu_plugin"


class ComposerMenuPlugin(AirflowPlugin):
    """Airflow plugin for adding Composer links as menu items in Airflow UI."""

    name = "ComposerMenuPlugin"
    external_views = [
        DAGS_GCC_EXTERNAL_VIEW,
        DAGS_GCS_EXTERNAL_VIEW,
        ENV_MON_EXTERNAL_VIEW,
        ENV_LOGS_EXTERNAL_VIEW,
        COMP_DOCS_EXTERNAL_VIEW,
    ]
