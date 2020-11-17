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

"""Composer. Increase length of user identifier columns in ``ab_user`` and ``ab_register_user`` tables

Revision ID: 5cdfae0b6cb3
Revises: 0979a47cb9bf
Create Date: 2023-01-18 16:21:09.420958

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "5cdfae0b6cb3"
down_revision = "0979a47cb9bf"
branch_labels = None
depends_on = "b0d31815b5a6"


def upgrade():
    """Increase length of user identifier columns in ab_user and ab_register_user tables"""
    with op.batch_alter_table("ab_user") as batch_op:
        batch_op.alter_column("first_name", type_=sa.String(256))
        batch_op.alter_column("last_name", type_=sa.String(256))
        batch_op.alter_column("username", type_=sa.String(512))
        batch_op.alter_column("email", type_=sa.String(512))
    with op.batch_alter_table("ab_register_user") as batch_op:
        batch_op.alter_column("first_name", type_=sa.String(256))
        batch_op.alter_column("last_name", type_=sa.String(256))
        batch_op.alter_column("username", type_=sa.String(512))
        batch_op.alter_column("email", type_=sa.String(512))


def downgrade():
    """Revert length of user identifier columns in ab_user and ab_register_user tables"""
    _ = op.get_bind()
    with op.batch_alter_table("ab_user") as batch_op:
        batch_op.alter_column("first_name", type_=sa.String(64))
        batch_op.alter_column("last_name", type_=sa.String(64))
        batch_op.alter_column("username", type_=sa.String(256))
        batch_op.alter_column("email", type_=sa.String(256))
    with op.batch_alter_table("ab_register_user") as batch_op:
        batch_op.alter_column("first_name", type_=sa.String(64))
        batch_op.alter_column("last_name", type_=sa.String(64))
        batch_op.alter_column("username", type_=sa.String(256))
        batch_op.alter_column("email", type_=sa.String(256))
