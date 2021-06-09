#
# Copyright 2021 Google LLC
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

import unittest
from unittest import mock

from parameterized import parameterized

from airflow import settings
from airflow.composer.utils import (
    get_composer_version,
    is_composer_v1,
    is_serverless_composer,
    is_triggerer_enabled,
)


class TestUtils(unittest.TestCase):
    @mock.patch.dict("os.environ", COMPOSER_VERSION="1.16.6")
    def test_get_composer_version(self):
        self.assertEqual(get_composer_version(), "1.16.6")

    def test_is_composer_v1(self):
        with mock.patch.dict("os.environ", COMPOSER_VERSION="1.16.6"):
            self.assertTrue(is_composer_v1())

        with mock.patch.dict("os.environ", COMPOSER_VERSION="2.0.0-preview.0"):
            self.assertFalse(is_composer_v1())

        with mock.patch.dict("os.environ", clear=True):
            self.assertFalse(is_composer_v1())

    def test_is_triggerer_enabled_default(self):
        self.assertFalse(is_triggerer_enabled())

    @parameterized.expand(
        [
            ("", False),
            ("1.20.12", False),
            ("2.0.0", False),
            ("2.4.21", False),
            ("2.5.0", True),
            ("2.5.0-preview.0", True),
            ("2.5.0-preview.1", True),
            ("2.15.0", True),
            ("3.0.0", True),
            ("10.0.0", True),
        ]
    )
    def test_is_serverless_composer(self, composer_version, expected_result):
        with mock.patch.dict("os.environ", {"COMPOSER_VERSION": composer_version}):
            self.assertEqual(is_serverless_composer(), expected_result)

    @mock.patch("airflow.composer.utils.initialize", autospec=True)
    def test_initialize(self, initialize_mock):
        settings.initialize()

        initialize_mock.assert_called_once()
