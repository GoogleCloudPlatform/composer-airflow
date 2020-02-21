#
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

"""
This module provides helper code to make type annotation within Airflow
codebase easier.
"""

try:
    # Protocol is only added to typing module starting from python 3.8
    # we can safely remove this shim import after Airflow drops support for
    # <3.8
    from typing import Protocol, runtime_checkable  # noqa # pylint: disable=unused-import
except ImportError:
    from typing_extensions import Protocol, runtime_checkable  # type: ignore # noqa
