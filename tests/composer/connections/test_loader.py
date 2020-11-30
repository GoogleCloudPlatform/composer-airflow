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
import unittest

from flask_appbuilder.fieldwidgets import BS3TextFieldWidget

from airflow.composer.connections.loader import (
    get_connection_field_behaviours,
    get_connection_form_widgets,
    get_connection_types,
)


class TestLoader(unittest.TestCase):
    def test_get_connection_types(self):
        connection_types = get_connection_types()

        self.assertIn(("aws", "Amazon Web Services"), connection_types)
        self.assertIn(("snowflake", "Snowflake"), connection_types)

    def test_get_connection_form_widgets(self):
        connection_form_widgets = get_connection_form_widgets()

        jdbc_drv_path_widget = connection_form_widgets["extra__jdbc__drv_path"]
        self.assertEqual(jdbc_drv_path_widget.connection_class, "JdbcHook")
        self.assertEqual(jdbc_drv_path_widget.package_name, "apache-airflow-providers-jdbc")
        self.assertEqual(jdbc_drv_path_widget.field.args, ("Driver Path",))
        self.assertIsInstance(jdbc_drv_path_widget.field.kwargs["widget"], BS3TextFieldWidget)

    def test_get_connection_field_behaviours(self):
        connection_field_behaviours = get_connection_field_behaviours()

        self.assertEqual(
            connection_field_behaviours["jdbc"],
            {
                "hidden_fields": ["port", "schema", "extra"],
                "relabeling": {"host": "Connection URL"},
            },
        )
