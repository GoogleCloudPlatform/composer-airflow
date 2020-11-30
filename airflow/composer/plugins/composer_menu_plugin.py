from __future__ import annotations

import os
from urllib.parse import urlencode

from airflow.plugins_manager import AirflowPlugin
from airflow.security import permissions

RESOURCE_COMPOSER_MENU = "Composer Menu"
RESOURCE_DAGS_IN_GCC = "DAGs in Cloud Console"
RESOURCE_DAGS_IN_GCS = "DAGs in Cloud Storage"
RESOURCE_ENVIRONMENT_MONITORING = "Environment Monitoring"
RESOURCE_ENVIRONMENT_LOGS = "Environment Logs"
RESOURCE_COMPOSER_DOCS = "Composer Documentation"

COMPOSER_MENU_PLUGIN_PERMISSIONS = [
    (permissions.ACTION_CAN_ACCESS_MENU, RESOURCE_COMPOSER_MENU),
    (permissions.ACTION_CAN_ACCESS_MENU, RESOURCE_DAGS_IN_GCC),
    (permissions.ACTION_CAN_ACCESS_MENU, RESOURCE_DAGS_IN_GCS),
    (permissions.ACTION_CAN_ACCESS_MENU, RESOURCE_ENVIRONMENT_MONITORING),
    (permissions.ACTION_CAN_ACCESS_MENU, RESOURCE_ENVIRONMENT_LOGS),
    (permissions.ACTION_CAN_ACCESS_MENU, RESOURCE_COMPOSER_DOCS),
]

MENU_CATEGORY_NAME = "Composer"

# No url encoding needed for COMPOSER_ENVIRONMENT. From Composer docs:
#   'The name must start with a lowercase letter followed by up to 62 lowercase letters,
#   numbers, or hyphens, and cannot end with a hyphen.'
ENVIRONMENT_DETAILS_LINK = (
    "https://console.cloud.google.com"
    f"/composer/environments/detail/{os.environ.get('COMPOSER_LOCATION')}"
    f"/{os.environ.get('COMPOSER_ENVIRONMENT')}"
    "{tab}"
    "?" + urlencode({"project": os.environ.get("GCP_PROJECT")})
)
DAGS_IN_GCC_LINK = ENVIRONMENT_DETAILS_LINK.format(tab="/dags")
DAGS_IN_GCS_LINK = f"https://console.cloud.google.com/storage/browser/{os.environ.get('GCS_BUCKET')}/dags"
ENVIRONMENT_MONITORING_LINK = ENVIRONMENT_DETAILS_LINK.format(tab="/monitoring")
ENVIRONMENT_LOGS_LINK = ENVIRONMENT_DETAILS_LINK.format(tab="/logs")
COMPOSER_DOCS_LINK = "https://cloud.google.com/composer/docs"

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


class ComposerMenuPlugin(AirflowPlugin):
    """Plugin for adding Composer links as menu items in Airflow UI"""

    name = "ComposerMenuPlugin"
    appbuilder_menu_items = [
        DAGS_GCC_APPBUILDER_MITEM,
        DAGS_GCS_APPBUILDER_MITEM,
        ENV_MON_APPBUILDER_MITEM,
        ENV_LOGS_APPBUILDER_MITEM,
        COMP_DOCS_APPBUILDER_MITEM,
    ]
