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

"""Change type of DagCode.source_code column

Revision ID: 7624214387e4
Revises: 952da73b5eff
Create Date: 2020-10-19 21:14:50.537315

"""

# revision identifiers, used by Alembic.
revision = '7624214387e4'
down_revision = '952da73b5eff'
branch_labels = None
depends_on = None

from alembic import op  # noqa
import sqlalchemy as sa   # noqa
from sqlalchemy.dialects import mysql  # noqa


def upgrade():
    conn = op.get_bind()
    if conn.dialect.name == 'mysql':
        conn.execute('truncate table dag_code')
        op.drop_column(table_name='dag_code', column_name='source_code')
        op.add_column(
            table_name='dag_code',
            column=sa.Column(
                'source_code', mysql.MEDIUMTEXT(unicode=True), nullable=False))


def downgrade():
    conn = op.get_bind()
    if conn.dialect.name == 'mysql':
        op.drop_column(table_name='dag_code', column_name='source_code')
        op.add_column(
            table_name='dag_code',
            column=sa.Column('source_code', sa.UnicodeText(), nullable=False))
