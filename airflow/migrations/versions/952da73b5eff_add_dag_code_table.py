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

"""add dag_code table

Revision ID: 952da73b5eff
Revises: d38e04c12aa2
Create Date: 2020-02-28 14:06:09.539934

"""

# revision identifiers, used by Alembic.
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.dagcode import DagCode

revision = '952da73b5eff'
down_revision = 'd38e04c12aa2'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    """Apply add source code table"""
    op.create_table('dag_code',  # pylint: disable=no-member
                    sa.Column('fileloc_hash', sa.BigInteger(), nullable=False, primary_key=True, autoincrement=False),
                    sa.Column('fileloc', sa.String(length=2000), nullable=False),
                    sa.Column('source_code', sa.UnicodeText(), nullable=False),
                    sa.Column('last_updated', sa.TIMESTAMP(timezone=True), nullable=False))

    conn = op.get_bind()
    if conn.dialect.name != 'sqlite':
        op.drop_index('idx_fileloc_hash', 'serialized_dag')
        op.drop_column(table_name='serialized_dag', column_name='fileloc_hash')
        op.add_column(
            table_name='serialized_dag',
            column=sa.Column('fileloc_hash', sa.BigInteger(), nullable=False))
        op.create_index(   # pylint: disable=no-member
            'idx_fileloc_hash', 'serialized_dag', ['fileloc_hash'])

    sessionmaker = sa.orm.sessionmaker()
    session = sessionmaker(bind=conn)
    serialized_dags = session.query(SerializedDagModel).all()
    for dag in serialized_dags:
        dag.fileloc_hash = DagCode.dag_fileloc_hash(dag.fileloc)
        session.merge(dag)
    session.commit()


def downgrade():
    """Unapply add source code table"""
    op.drop_table('dag_code')
