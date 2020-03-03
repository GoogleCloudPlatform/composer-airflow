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
import datetime
import logging
import os
import struct

from sqlalchemy import BigInteger, Column, String, UnicodeText, and_, exists

from airflow.exceptions import AirflowException
from airflow.models import Base
from airflow.utils import timezone
from airflow.utils.file import correct_maybe_zipped
from airflow.utils.file import open_maybe_zipped
from airflow.utils.session import provide_session
from airflow.utils.sqlalchemy import UtcDateTime

log = logging.getLogger(__name__)


class DagCode(Base):
    """A table for DAGs code.

    dag_code table contains code of DAG files synchronized by scheduler.
    This feature is controlled by:

    * ``[core] store_dag_code = True``: enable this feature

    For details on dag serialization see SerializedDagModel
    """
    __tablename__ = 'dag_code'

    fileloc_hash = Column(BigInteger, nullable=False, primary_key=True)
    # Not indexed because the max length of fileloc exceeds the limit of indexing.
    fileloc = Column(String(2000), nullable=False)
    last_updated = Column(UtcDateTime, nullable=False)
    source_code = Column(UnicodeText(), nullable=False)

    def __init__(self, full_filepath):
        self.fileloc = full_filepath
        self.fileloc_hash = DagCode.dag_fileloc_hash(self.fileloc)
        self.last_updated = timezone.utcnow()
        self.source_code = DagCode._read_code(self.fileloc)

    @classmethod
    def _read_code(cls, fileloc):
        with open_maybe_zipped(fileloc, 'r') as source:
            source_code = source.read()
        return source_code

    @provide_session
    def sync_to_db(self, session=None):
        """Writes code into database.

        :param session: ORM Session
        """
        old_version = session.query(
            DagCode.fileloc, DagCode.fileloc_hash, DagCode.last_updated)\
            .filter(DagCode.fileloc_hash == self.fileloc_hash)\
            .first()

        if old_version and old_version.fileloc != self.fileloc:
            raise AirflowException(
                "Filename '{}' causes a hash collision in the database."
                " Please rename the file.".format(self.fileloc))

        file_modified = datetime.datetime\
            .fromtimestamp(
                os.path.getmtime(correct_maybe_zipped(self.fileloc)),
                tz=timezone.utc)

        if old_version and (file_modified - datetime.timedelta(seconds=120)) <\
                old_version.last_updated:
            return

        session.merge(self)

    @classmethod
    @provide_session
    def remove_deleted_code(cls, alive_dag_filelocs, session=None):
        """Deletes code not included in alive_dag_filelocs.

        :param alive_dag_filelocs: file paths of alive DAGs
        :param session: ORM Session
        """
        alive_fileloc_hashes = [
            cls.dag_fileloc_hash(fileloc) for fileloc in alive_dag_filelocs]

        log.debug("Deleting code from table %s", cls.__tablename__)

        session.execute(
            cls.__table__.delete().where(
                and_(cls.fileloc_hash.notin_(alive_fileloc_hashes),
                     cls.fileloc.notin_(alive_dag_filelocs))))

    @classmethod
    @provide_session
    def has_dag(cls, fileloc, session=None):
        """Checks a file exist in dag_code table.

        :param fileloc: the file to check
        :param session: ORM Session
        """
        fileloc_hash = cls.dag_fileloc_hash(fileloc)
        return session.query(exists().where(cls.fileloc_hash == fileloc_hash))\
            .scalar()

    @staticmethod
    def dag_fileloc_hash(full_filepath):
        """"Hashing file location for indexing.

        :param full_filepath: full filepath of DAG file
        :return: hashed full_filepath
        """
        # hashing is needed because the length of fileloc is 2000 as an Airflow convention,
        # which is over the limit of indexing. If we can reduce the length of fileloc, then
        # hashing is not needed.
        import hashlib
        return struct.unpack('>Q', hashlib.sha1(
            full_filepath.encode('utf-8')).digest()[-8:])[0] >> 8
