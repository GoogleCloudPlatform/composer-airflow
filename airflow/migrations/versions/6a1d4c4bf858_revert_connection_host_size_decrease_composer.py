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

"""Composer. Revert back length of connection.host column

The length of this column accidentally was reduced in 5962f262d786 custom
Composer migration. Here we revert back length of it.

Revision ID: 6a1d4c4bf858
Revises: 31cbfc73ceed
Create Date: 2021-10-20 07:44:05.123456

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6a1d4c4bf858"
down_revision = "31cbfc73ceed"
branch_labels = None
depends_on = None


def upgrade():
    """Apply migration"""
    with op.batch_alter_table("connection") as batch_op:
        batch_op.alter_column(column_name="host", type_=sa.String(500))


def downgrade():
    """Unapply migration"""
