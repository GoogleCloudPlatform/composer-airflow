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

# List of permissions to be added for all RBAC roles. Defined in the following format:
# [(permissions.ACTION_CAN_ACCESS_MENU, RESOURCE_COMPOSER_MENU), ...]
# The list is supposed to be populated in other patches (e.g. "webserver patch"), and used in "rbac patch"
# where actually permissions are added to the RBAC roles.
ALL_RBAC_ROLES_EXTRA_PERMISSIONS = []
