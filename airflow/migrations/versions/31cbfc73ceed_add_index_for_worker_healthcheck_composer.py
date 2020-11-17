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

"""Composer. Add index for worker healthcheck

Revision ID: 31cbfc73ceed
Revises: 5962f262d786
Create Date: 2020-10-13 20:04:36.783436

"""
from alembic import op
from sqlalchemy.engine.reflection import Inspector

# revision identifiers, used by Alembic.
revision = '31cbfc73ceed'
down_revision = '5962f262d786'
branch_labels = None
depends_on = None


def upgrade():  # noqa: D103
    connection = op.get_bind()
    inspector = Inspector.from_engine(connection)
    indices = inspector.get_indexes('task_instance')
    for index in indices:
        if index['name'] == 'ti_worker_healthcheck':
            return

    op.create_index('ti_worker_healthcheck', 'task_instance', ['end_date', 'hostname', 'state'], unique=False)


def downgrade():  # noqa: D103
    op.drop_index('ti_worker_healthcheck', table_name='task_instance')
