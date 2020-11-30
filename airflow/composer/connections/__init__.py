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
"""Package that contains Airflow connection assets required for Composer 1.*.* versions.

There are following connection assets located inside this package:
- connection_types[.dill,.pprint]
- connection_form_widgets[.dill,.pprint]
- connection_field_behaviours[.dill,.pprint]

Dill files are binary dill-serialized objects that are going to be unpacked and
used on Airflow "Connections" page (see loader.py).
Pprint files are used to track content difference of appropriate dill files.
"""
