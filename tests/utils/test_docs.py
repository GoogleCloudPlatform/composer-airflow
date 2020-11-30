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
from __future__ import annotations

from unittest import mock

import pytest

from airflow.utils.docs import get_doc_url_for_provider, get_docs_url


class TestGetDocsUrl:
    @pytest.mark.parametrize(
        "version, page, expected_url",
        [
            (
                "2.0.0.dev0",
                None,
                "http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com/docs/"
                "apache-airflow/latest/",
            ),
            (
                "2.0.0.dev0",
                "migration.html",
                "http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com/docs/"
                "apache-airflow/latest/migration.html",
            ),
            ("1.10.10", None, "https://airflow.apache.org/docs/apache-airflow/1.10.10/"),
            (
                "1.10.10",
                "project.html",
                "https://airflow.apache.org/docs/apache-airflow/1.10.10/project.html",
            ),
        ],
    )
    def test_should_return_link(self, version, page, expected_url):
        with mock.patch("airflow.version.version", version):
            assert expected_url == get_docs_url(page)

    @pytest.mark.parametrize(
        "provider_name, provider_version, expected_url",
        [
            (
                "apache-airflow-providers-google",
                "2023.4.13+composer",
                "https://github.com/GoogleCloudPlatform/composer-airflow/blob/providers-google-2023.4.13+composer/airflow/providers/google/CHANGELOG.rst",  # noqa: E501
            ),
            (
                "apache-airflow-providers-google",
                "8.9.0",
                "https://airflow.apache.org/docs/apache-airflow-providers-google/8.9.0/",
            ),
        ],
    )
    @mock.patch("importlib_metadata.metadata", autospec=True)
    def test_get_doc_url_for_provider(self, mock_metadata, provider_name, provider_version, expected_url):
        mock_metadata.return_value.get_all.return_value = None
        assert expected_url == get_doc_url_for_provider(provider_name, provider_version)
