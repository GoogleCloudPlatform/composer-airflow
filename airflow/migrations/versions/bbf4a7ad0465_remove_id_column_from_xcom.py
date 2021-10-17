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

"""Remove id column from xcom

Revision ID: bbf4a7ad0465
Revises: cf5dc11e79ad
Create Date: 2019-10-29 13:53:09.445943

"""

from collections import defaultdict

from alembic import op
from sqlalchemy import Column, Integer
from sqlalchemy.engine.reflection import Inspector

# revision identifiers, used by Alembic.
revision = 'bbf4a7ad0465'
down_revision = 'cf5dc11e79ad'
branch_labels = None
depends_on = None


def get_table_constraints(conn, table_name):
    """
    This function return primary and unique constraint
    along with column name. Some tables like `task_instance`
    is missing the primary key constraint name and the name is
    auto-generated by the SQL server. so this function helps to
    retrieve any primary or unique constraint name.

    :param conn: sql connection object
    :param table_name: table name
    :return: a dictionary of ((constraint name, constraint type), column name) of table
    :rtype: defaultdict(list)
    """
    query = f"""SELECT tc.CONSTRAINT_NAME , tc.CONSTRAINT_TYPE, ccu.COLUMN_NAME
     FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
     JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS ccu ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
     WHERE tc.TABLE_NAME = '{table_name}' AND
     (tc.CONSTRAINT_TYPE = 'PRIMARY KEY' or UPPER(tc.CONSTRAINT_TYPE) = 'UNIQUE')
    """
    result = conn.execute(query).fetchall()
    constraint_dict = defaultdict(list)
    for constraint, constraint_type, column in result:
        constraint_dict[(constraint, constraint_type)].append(column)
    return constraint_dict


def drop_column_constraints(operator, column_name, constraint_dict):
    """
    Drop a primary key or unique constraint

    :param operator: batch_alter_table for the table
    :param constraint_dict: a dictionary of ((constraint name, constraint type), column name) of table
    """
    for constraint, columns in constraint_dict.items():
        if column_name in columns:
            if constraint[1].lower().startswith("primary"):
                operator.drop_constraint(constraint[0], type_='primary')
            elif constraint[1].lower().startswith("unique"):
                operator.drop_constraint(constraint[0], type_='unique')


def create_constraints(operator, column_name, constraint_dict):
    """
    Create a primary key or unique constraint

    :param operator: batch_alter_table for the table
    :param constraint_dict: a dictionary of ((constraint name, constraint type), column name) of table
    """
    for constraint, columns in constraint_dict.items():
        if column_name in columns:
            if constraint[1].lower().startswith("primary"):
                operator.create_primary_key(constraint_name=constraint[0], columns=columns)
            elif constraint[1].lower().startswith("unique"):
                operator.create_unique_constraint(constraint_name=constraint[0], columns=columns)


def upgrade():
    """Apply Remove id column from xcom"""
    conn = op.get_bind()
    inspector = Inspector.from_engine(conn)

    with op.batch_alter_table('xcom') as bop:
        xcom_columns = [col.get('name') for col in inspector.get_columns("xcom")]
        if "id" in xcom_columns:
            if conn.dialect.name == 'mssql':
                constraint_dict = get_table_constraints(conn, "xcom")
                drop_column_constraints(bop, 'id', constraint_dict)
            bop.drop_column('id')
            bop.drop_index('idx_xcom_dag_task_date')
            # mssql doesn't allow primary keys with nullable columns
            if conn.dialect.name != 'mssql':
                bop.create_primary_key('pk_xcom', ['dag_id', 'task_id', 'key', 'execution_date'])


def downgrade():
    """Unapply Remove id column from xcom"""
    with op.batch_alter_table('xcom') as bop:
        bop.drop_constraint('pk_xcom', type_='primary')
        bop.add_column(Column('id', Integer, primary_key=True))
        bop.create_index('idx_xcom_dag_task_date', ['dag_id', 'task_id', 'key', 'execution_date'])
