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

from airflow.plugins_manager import AirflowPlugin, AirflowPluginSource, register_plugin

MENU_CATEGORY_NAME = "Composer"

# Resources.
RESOURCE_COMPOSER_MENU = "Composer Menu"
RESOURCE_DAGS_IN_GCC = "DAGs in Cloud Console"
RESOURCE_DAGS_IN_GCS = "DAGs in Cloud Storage"
RESOURCE_ENVIRONMENT_MONITORING = "Environment Monitoring"
RESOURCE_ENVIRONMENT_LOGS = "Environment Logs"
RESOURCE_COMPOSER_DOCS = "Composer Documentation"

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

# Menu items.
DAGS_GCC_APPBUILDER_MITEM = {
    "name": RESOURCE_DAGS_IN_GCC,
    "label": "DAGs in Cloud Console",
    "href": DAGS_IN_GCC_LINK,
    "category": RESOURCE_COMPOSER_MENU,
    "category_label": MENU_CATEGORY_NAME,
}
DAGS_GCS_APPBUILDER_MITEM = {
    "name": RESOURCE_DAGS_IN_GCS,
    "label": "DAGs in Cloud Storage",
    "href": DAGS_IN_GCS_LINK,
    "category": RESOURCE_COMPOSER_MENU,
    "category_label": MENU_CATEGORY_NAME,
}
ENV_MON_APPBUILDER_MITEM = {
    "name": RESOURCE_ENVIRONMENT_MONITORING,
    "label": "Environment Monitoring",
    "href": ENVIRONMENT_MONITORING_LINK,
    "category": RESOURCE_COMPOSER_MENU,
    "category_label": MENU_CATEGORY_NAME,
}
ENV_LOGS_APPBUILDER_MITEM = {
    "name": RESOURCE_ENVIRONMENT_LOGS,
    "label": "Environment Logs",
    "href": ENVIRONMENT_LOGS_LINK,
    "category": RESOURCE_COMPOSER_MENU,
    "category_label": MENU_CATEGORY_NAME,
}
COMP_DOCS_APPBUILDER_MITEM = {
    "name": RESOURCE_COMPOSER_DOCS,
    "label": "Composer Documentation",
    "href": COMPOSER_DOCS_LINK,
    "category": RESOURCE_COMPOSER_MENU,
    "category_label": MENU_CATEGORY_NAME,
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
    appbuilder_menu_items = [
        DAGS_GCC_APPBUILDER_MITEM,
        DAGS_GCS_APPBUILDER_MITEM,
        ENV_MON_APPBUILDER_MITEM,
        ENV_LOGS_APPBUILDER_MITEM,
        COMP_DOCS_APPBUILDER_MITEM,
    ]
