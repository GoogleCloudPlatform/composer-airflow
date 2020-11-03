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

"""add is_encrypted column to variable table

Revision ID: 1968acfc09e3
Revises: bba5a7cfc896
Create Date: 2016-02-02 17:20:55.692295

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '1968acfc09e3'
down_revision = 'bba5a7cfc896'
branch_labels = None
depends_on = None


def upgrade():  # noqa: D103
    op.add_column('variable', sa.Column('is_encrypted', sa.Boolean, default=False))


def downgrade():  # noqa: D103
    op.drop_column('variable', 'is_encrypted')
