from __future__ import annotations

import importlib
import io
import logging
import sys
from unittest import mock

from airflow.composer.redirect_callback_logs_utils import handle_callback
from airflow.config_templates import airflow_local_settings
from airflow.logging_config import configure_logging


def test_dag_callback_context_manager():
    def callback(x, y):
        sum = x + y
        logging.error("sum log is %s", sum)
        print("sum out is", sum)
        print("sum error is", sum, sys.stderr)

    with mock.patch.dict("os.environ", {"CONFIG_PROCESSOR_MANAGER_LOGGER": "True"}):
        importlib.reload(airflow_local_settings)
        configure_logging()

    processor_manager_log = logging.getLogger("airflow.processor_manager")
    stream = io.StringIO()
    new_handler = logging.StreamHandler(stream)
    new_handler.setFormatter(processor_manager_log.handlers[0].formatter)
    processor_manager_log.addHandler(new_handler)

    handle_callback(callback, processor_manager_log, 1, 2)

    assert "sum log is 3" in stream.getvalue()
    assert "sum out is 3" in stream.getvalue()
    assert "sum error is 3" in stream.getvalue()
