from __future__ import annotations

import os
from importlib import reload
from unittest import mock

import pytest

from airflow.composer.plugins import composer_menu_plugin
from airflow.www import app
from tests.test_utils.api_connexion_utils import create_user
from tests.test_utils.config import conf_vars
from tests.test_utils.www import check_content_in_response, client_with_login


class TestComposerMenuPlugin:
    @classmethod
    def setup_class(cls):
        with mock.patch.dict(
            os.environ,
            {
                "GCP_PROJECT": "test-project",
                "COMPOSER_LOCATION": "test-location",
                "COMPOSER_ENVIRONMENT": "test-env",
                "GCS_BUCKET": "test-location-test-env-bucket",
            },
        ), conf_vars(
            {
                ("webserver", "rbac_user_registration_role"): "Viewer",
            }
        ):
            reload(composer_menu_plugin)
            cls.app = app.create_app(testing=True)
            cls.app.config["WTF_CSRF_ENABLED"] = False

    @pytest.mark.parametrize(
        "expected_label, expected_href",
        [
            (
                "DAGs in Cloud Console",
                (
                    "https://console.cloud.google.com/composer/environments/detail/test-location/test-env"
                    "/dags?project=test-project"
                ),
            ),
            (
                "DAGs in Cloud Storage",
                "https://console.cloud.google.com/storage/browser/test-location-test-env-bucket/dags",
            ),
            (
                "Environment Monitoring",
                (
                    "https://console.cloud.google.com/composer/environments/detail/test-location/test-env"
                    "/monitoring?project=test-project"
                ),
            ),
            (
                "Environment Logs",
                (
                    "https://console.cloud.google.com/composer/environments/detail/test-location/test-env"
                    "/logs?project=test-project"
                ),
            ),
            ("Composer Documentation", "https://cloud.google.com/composer/docs"),
        ],
    )
    def test_menu_links(self, expected_label, expected_href):
        menu_items = composer_menu_plugin.ComposerMenuPlugin().appbuilder_menu_items

        assert expected_label in [menu_item["label"] for menu_item in menu_items]
        for menu_item in menu_items:
            if menu_item["label"] == expected_label:
                assert menu_item["href"] == expected_href

    def test_menu_items_under_same_category(self):
        for menu_item in composer_menu_plugin.ComposerMenuPlugin().appbuilder_menu_items:
            assert menu_item["category_label"] == "Composer"

    def test_composer_menu_visible(self):
        test_user = {
            "username": "test_composer_menu_visible",
            "email": "test_composer_menu_visible@test.com",
            "role_name": "TestRole",
            "permissions": [
                ("can_read", "Website"),
                ("menu_access", "Composer Menu"),
                ("menu_access", "DAGs in Cloud Console"),
                ("menu_access", "DAGs in Cloud Storage"),
                ("menu_access", "Environment Monitoring"),
                ("menu_access", "Environment Logs"),
                ("menu_access", "Composer Documentation"),
            ],
        }
        create_user(self.app, **test_user)
        client = client_with_login(
            self.app, username="test_composer_menu_visible", password="test_composer_menu_visible"
        )

        resp = client.get("/home")

        expected_link_texts = [
            "DAGs in Cloud Console",
            "DAGs in Cloud Storage",
            "Environment Monitoring",
            "Environment Logs",
            "Composer Documentation",
        ]
        for text in expected_link_texts:
            check_content_in_response(text, resp)
