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

"""Composer. Add index for worker healthcheck second time

This index was already added in 31cbfc73ceed Composer migration, but after
7b2661a43ba3 community migration this index is removed and here we add it back.

Revision ID: 0979a47cb9bf
Revises: 6a1d4c4bf858
Create Date: 2021-12-23 11:56:05.123456

"""

from alembic import op
from sqlalchemy.engine.reflection import Inspector

# revision identifiers, used by Alembic.
revision = '0979a47cb9bf'
down_revision = '6a1d4c4bf858'
branch_labels = None
depends_on = '7b2661a43ba3'


def upgrade():
    connection = op.get_bind()
    inspector = Inspector.from_engine(connection)
    indices = inspector.get_indexes('task_instance')
    for index in indices:
        if index['name'] == 'ti_worker_healthcheck':
            return

    op.create_index('ti_worker_healthcheck', 'task_instance', ['end_date', 'hostname', 'state'], unique=False)


def downgrade():
    op.drop_index('ti_worker_healthcheck', table_name='task_instance')
