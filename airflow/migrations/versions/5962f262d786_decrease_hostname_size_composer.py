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

"""Composer. Adjust length of hostname columns

As we use it in index and there's limit for index size in MySQL, we have to
shorten length of this column.

Revision ID: 5962f262d786
Revises:
Create Date: 2021-03-03 15:41:07.261119

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '5962f262d786'
down_revision = None
branch_labels = None
depends_on = 'e3a246e0dc1'


def upgrade():
    """Apply decrease_hostname_size"""
    with op.batch_alter_table('connection') as batch_op:
        batch_op.alter_column(column_name='host', type_=sa.String(100))
    with op.batch_alter_table('job') as batch_op:
        batch_op.alter_column(column_name='hostname', type_=sa.String(100))
    with op.batch_alter_table('task_instance') as batch_op:
        batch_op.alter_column(column_name='hostname', type_=sa.String(100))


def downgrade():
    """Unapply decrease_hostname_size"""
    pass
