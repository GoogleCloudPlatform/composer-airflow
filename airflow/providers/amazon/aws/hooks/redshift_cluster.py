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
from __future__ import annotations

from typing import Any, Sequence

from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class RedshiftHook(AwsBaseHook):
    """
    Interact with AWS Redshift, using the boto3 library

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    template_fields: Sequence[str] = ('cluster_identifier',)

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "redshift"
        super().__init__(*args, **kwargs)

    def create_cluster(
        self,
        cluster_identifier: str,
        node_type: str,
        master_username: str,
        master_user_password: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Creates a new cluster with the specified parameters

        :param cluster_identifier: A unique identifier for the cluster.
        :param node_type: The node type to be provisioned for the cluster.
            Valid Values: ``ds2.xlarge``, ``ds2.8xlarge``, ``dc1.large``,
            ``dc1.8xlarge``, ``dc2.large``, ``dc2.8xlarge``, ``ra3.xlplus``,
            ``ra3.4xlarge``, and ``ra3.16xlarge``.
        :param master_username: The username associated with the admin user account
            for the cluster that is being created.
        :param master_user_password: password associated with the admin user account
            for the cluster that is being created.
        :param params: Remaining AWS Create cluster API params.
        """
        try:
            response = self.get_conn().create_cluster(
                ClusterIdentifier=cluster_identifier,
                NodeType=node_type,
                MasterUsername=master_username,
                MasterUserPassword=master_user_password,
                **params,
            )
            return response
        except ClientError as e:
            raise e

    # TODO: Wrap create_cluster_snapshot
    def cluster_status(self, cluster_identifier: str) -> str:
        """
        Return status of a cluster

        :param cluster_identifier: unique identifier of a cluster
        :param skip_final_cluster_snapshot: determines cluster snapshot creation
        :param final_cluster_snapshot_identifier: Optional[str]
        """
        try:
            response = self.get_conn().describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters']
            return response[0]['ClusterStatus'] if response else None
        except self.get_conn().exceptions.ClusterNotFoundFault:
            return 'cluster_not_found'

    def delete_cluster(
        self,
        cluster_identifier: str,
        skip_final_cluster_snapshot: bool = True,
        final_cluster_snapshot_identifier: str | None = None,
    ):
        """
        Delete a cluster and optionally create a snapshot

        :param cluster_identifier: unique identifier of a cluster
        :param skip_final_cluster_snapshot: determines cluster snapshot creation
        :param final_cluster_snapshot_identifier: name of final cluster snapshot
        """
        final_cluster_snapshot_identifier = final_cluster_snapshot_identifier or ''

        response = self.get_conn().delete_cluster(
            ClusterIdentifier=cluster_identifier,
            SkipFinalClusterSnapshot=skip_final_cluster_snapshot,
            FinalClusterSnapshotIdentifier=final_cluster_snapshot_identifier,
        )
        return response['Cluster'] if response['Cluster'] else None

    def describe_cluster_snapshots(self, cluster_identifier: str) -> list[str] | None:
        """
        Gets a list of snapshots for a cluster

        :param cluster_identifier: unique identifier of a cluster
        """
        response = self.get_conn().describe_cluster_snapshots(ClusterIdentifier=cluster_identifier)
        if 'Snapshots' not in response:
            return None
        snapshots = response['Snapshots']
        snapshots = [snapshot for snapshot in snapshots if snapshot["Status"]]
        snapshots.sort(key=lambda x: x['SnapshotCreateTime'], reverse=True)
        return snapshots

    def restore_from_cluster_snapshot(self, cluster_identifier: str, snapshot_identifier: str) -> str:
        """
        Restores a cluster from its snapshot

        :param cluster_identifier: unique identifier of a cluster
        :param snapshot_identifier: unique identifier for a snapshot of a cluster
        """
        response = self.get_conn().restore_from_cluster_snapshot(
            ClusterIdentifier=cluster_identifier, SnapshotIdentifier=snapshot_identifier
        )
        return response['Cluster'] if response['Cluster'] else None

    def create_cluster_snapshot(
        self, snapshot_identifier: str, cluster_identifier: str, retention_period: int = -1
    ) -> str:
        """
        Creates a snapshot of a cluster

        :param snapshot_identifier: unique identifier for a snapshot of a cluster
        :param cluster_identifier: unique identifier of a cluster
        :param retention_period: The number of days that a manual snapshot is retained.
            If the value is -1, the manual snapshot is retained indefinitely.
        """
        response = self.get_conn().create_cluster_snapshot(
            SnapshotIdentifier=snapshot_identifier,
            ClusterIdentifier=cluster_identifier,
            ManualSnapshotRetentionPeriod=retention_period,
        )
        return response['Snapshot'] if response['Snapshot'] else None

    def get_cluster_snapshot_status(self, snapshot_identifier: str, cluster_identifier: str):
        """
        Return Redshift cluster snapshot status. If cluster snapshot not found return ``None``

        :param snapshot_identifier: A unique identifier for the snapshot that you are requesting
        :param cluster_identifier: The unique identifier of the cluster the snapshot was created from
        """
        try:
            response = self.get_conn().describe_cluster_snapshots(
                ClusterIdentifier=cluster_identifier,
                SnapshotIdentifier=snapshot_identifier,
            )
            snapshot = response.get("Snapshots")[0]
            snapshot_status: str = snapshot.get("Status")
            return snapshot_status
        except self.get_conn().exceptions.ClusterSnapshotNotFoundFault:
            return None
