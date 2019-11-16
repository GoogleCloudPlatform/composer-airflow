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

"""alembic merge

Revision ID: d3d64fa99562
Revises: 939bb1e647c8, d38e04c12aa2, a56c9515abdc
Create Date: 2020-03-02 13:09:19.426468

"""

# revision identifiers, used by Alembic.
revision = 'd3d64fa99562'
down_revision = ('939bb1e647c8', 'd38e04c12aa2', 'a56c9515abdc')
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    pass


def downgrade():
    pass
