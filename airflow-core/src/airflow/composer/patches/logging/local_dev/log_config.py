from __future__ import annotations

from copy import deepcopy

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

TASK_HANDLER_CLASS = "airflow.composer.patches.logging.local_dev.task_handler.ComposerLocalDevTaskHandler"

LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)
LOGGING_CONFIG["handlers"]["task"]["class"] = TASK_HANDLER_CLASS
