# -*- coding: utf-8 -*-
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
from unittest import TestCase
import importlib


from parameterized import parameterized


class TestMovingCoreToContrib(TestCase):
    def assert_is_subclass(self, clazz, other):
        self.assertTrue(issubclass(clazz, other))

    def assert_proper_import(self, new_resource: str, old_resource: str):
        old_path, _, old_class_name = old_resource.rpartition(".")
        new_path, _, new_class_name = new_resource.rpartition(".")
        with self.assertWarns(DeprecationWarning):
            # Reload to see deprecation warning each time
            old_module = importlib.reload(importlib.import_module(old_path))
        new_module = importlib.import_module(new_path)

        new_clazz = getattr(new_module, new_class_name)
        old_clazz = getattr(old_module, old_class_name)
        self.assert_is_subclass(new_clazz, old_clazz)

    @parameterized.expand(
        [
            (
                "airflow.gcp.hooks.cloud_build.CloudBuildHook",
                "airflow.contrib.hooks.gcp_cloud_build_hook.CloudBuildHook",
            ),
            (
                "airflow.gcp.hooks.bigtable.BigtableHook",
                "airflow.contrib.hooks.gcp_bigtable_hook.BigtableHook",
            ),
            (
                "airflow.gcp.hooks.compute.GceHook",
                "airflow.contrib.hooks.gcp_compute_hook.GceHook",
            ),
            (
                "airflow.gcp.hooks.kubernetes_engine.GKEClusterHook",
                "airflow.contrib.hooks.gcp_container_hook.GKEClusterHook",
            ),
            (
                "airflow.gcp.hooks.dataflow.DataFlowHook",
                "airflow.contrib.hooks.gcp_dataflow_hook.DataFlowHook",
            ),
            (
                "airflow.gcp.hooks.datastore.DatastoreHook",
                "airflow.contrib.hooks.datastore_hook.DatastoreHook",
            ),
            (
                "airflow.gcp.hooks.dlp.CloudDLPHook",
                "airflow.contrib.hooks.gcp_dlp_hook.CloudDLPHook",
            ),
            (
                "airflow.gcp.hooks.functions.GcfHook",
                "airflow.contrib.hooks.gcp_function_hook.GcfHook",
            ),
            (
                "airflow.gcp.hooks.kms.GoogleCloudKMSHook",
                "airflow.contrib.hooks.gcp_kms_hook.GoogleCloudKMSHook",
            ),
            (
                "airflow.gcp.hooks.mlengine.MLEngineHook",
                "airflow.contrib.hooks.gcp_mlengine_hook.MLEngineHook",
            ),
            (
                "airflow.gcp.hooks.natural_language.CloudNaturalLanguageHook",
                "airflow.contrib.hooks.gcp_natural_language_hook.CloudNaturalLanguageHook",
            ),
            (
                "airflow.gcp.hooks.pubsub.PubSubHook",
                "airflow.contrib.hooks.gcp_pubsub_hook.PubSubHook",
            ),
            (
                "airflow.gcp.hooks.spanner.CloudSpannerHook",
                "airflow.contrib.hooks.gcp_spanner_hook.CloudSpannerHook",
            ),
            (
                "airflow.gcp.hooks.speech_to_text.GCPSpeechToTextHook",
                "airflow.contrib.hooks.gcp_speech_to_text_hook.GCPSpeechToTextHook",
            ),
            (
                "airflow.gcp.hooks.cloud_sql.CloudSqlHook",
                "airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook",
            ),
            (
                "airflow.gcp.hooks.tasks.CloudTasksHook",
                "airflow.contrib.hooks.gcp_tasks_hook.CloudTasksHook",
            ),
            (
                "airflow.gcp.hooks.text_to_speech.GCPTextToSpeechHook",
                "airflow.contrib.hooks.gcp_text_to_speech_hook.GCPTextToSpeechHook",
            ),
            (
                "airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook",
                "airflow.contrib.hooks.gcp_transfer_hook.GCPTransferServiceHook",
            ),
            (
                "airflow.gcp.hooks.translate.CloudTranslateHook",
                "airflow.contrib.hooks.gcp_translate_hook.CloudTranslateHook",
            ),
            (
                "airflow.gcp.hooks.video_intelligence.CloudVideoIntelligenceHook",
                "airflow.contrib.hooks.gcp_video_intelligence_hook.CloudVideoIntelligenceHook",
            ),
            (
                "airflow.gcp.hooks.vision.CloudVisionHook",
                "airflow.contrib.hooks.gcp_vision_hook.CloudVisionHook",
            ),
            (
                "airflow.gcp.hooks.dataproc.DataProcHook",
                "airflow.contrib.hooks.gcp_dataproc_hook.DataProcHook",
            ),
        ]
    )
    def test_hooks_paths(self, new_path: str, old_path: str):
        self.assert_proper_import(new_path, old_path)

    @parameterized.expand(
        [
            (
                "airflow.operators.adls_to_gcs.AdlsToGoogleCloudStorageOperator",
                "airflow.contrib.operators.adls_to_gcs.AdlsToGoogleCloudStorageOperator",
            ),
            (
                "airflow.gcp.operators.dataflow.DataFlowJavaOperator",
                "airflow.contrib.operators.dataflow_operator.DataFlowJavaOperator",
            ),
            (
                "airflow.gcp.operators.dataflow.DataFlowPythonOperator",
                "airflow.contrib.operators.dataflow_operator.DataFlowPythonOperator",
            ),
            (
                "airflow.gcp.operators.dataflow.DataflowTemplateOperator",
                "airflow.contrib.operators.dataflow_operator.DataflowTemplateOperator",
            ),
            (
                "airflow.gcp.operators.datastore.DatastoreExportOperator",
                "airflow.contrib.operators.datastore_export_operator.DatastoreExportOperator",
            ),
            (
                "airflow.gcp.operators.datastore.DatastoreImportOperator",
                "airflow.contrib.operators.datastore_import_operator.DatastoreImportOperator",
            ),
            (
                "airflow.operators.local_to_gcs.FileToGoogleCloudStorageOperator",
                "airflow.contrib.operators.file_to_gcs.FileToGoogleCloudStorageOperator",
            ),
            (
                "airflow.gcp.operators.bigtable.BigtableClusterUpdateOperator",
                "airflow.contrib.operators.gcp_bigtable_operator.BigtableClusterUpdateOperator",
            ),
            (
                "airflow.gcp.operators.bigtable.BigtableInstanceCreateOperator",
                "airflow.contrib.operators.gcp_bigtable_operator.BigtableInstanceCreateOperator",
            ),
            (
                "airflow.gcp.operators.bigtable.BigtableInstanceDeleteOperator",
                "airflow.contrib.operators.gcp_bigtable_operator.BigtableInstanceDeleteOperator",
            ),
            (
                "airflow.gcp.operators.bigtable.BigtableTableCreateOperator",
                "airflow.contrib.operators.gcp_bigtable_operator.BigtableTableCreateOperator",
            ),
            (
                "airflow.gcp.operators.bigtable.BigtableTableDeleteOperator",
                "airflow.contrib.operators.gcp_bigtable_operator.BigtableTableDeleteOperator",
            ),
            (
                "airflow.gcp.operators.cloud_build.CloudBuildCreateBuildOperator",
                "airflow.contrib.operators.gcp_cloud_build_operator.CloudBuildCreateBuildOperator",
            ),
            (
                "airflow.gcp.operators.compute.GceBaseOperator",
                "airflow.contrib.operators.gcp_compute_operator.GceBaseOperator",
            ),
            (
                "airflow.gcp.operators.compute.GceInstanceGroupManagerUpdateTemplateOperator",
                "airflow.contrib.operators.gcp_compute_operator."
                "GceInstanceGroupManagerUpdateTemplateOperator",
            ),
            (
                "airflow.gcp.operators.compute.GceInstanceStartOperator",
                "airflow.contrib.operators.gcp_compute_operator.GceInstanceStartOperator",
            ),
            (
                "airflow.gcp.operators.compute.GceInstanceStopOperator",
                "airflow.contrib.operators.gcp_compute_operator.GceInstanceStopOperator",
            ),
            (
                "airflow.gcp.operators.compute.GceInstanceTemplateCopyOperator",
                "airflow.contrib.operators.gcp_compute_operator.GceInstanceTemplateCopyOperator",
            ),
            (
                "airflow.gcp.operators.compute.GceSetMachineTypeOperator",
                "airflow.contrib.operators.gcp_compute_operator.GceSetMachineTypeOperator",
            ),
            (
                "airflow.gcp.operators.kubernetes_engine.GKEClusterCreateOperator",
                "airflow.contrib.operators.gcp_container_operator.GKEClusterCreateOperator",
            ),
            (
                "airflow.gcp.operators.kubernetes_engine.GKEClusterDeleteOperator",
                "airflow.contrib.operators.gcp_container_operator.GKEClusterDeleteOperator",
            ),
            (
                "airflow.gcp.operators.kubernetes_engine.GKEPodOperator",
                "airflow.contrib.operators.gcp_container_operator.GKEPodOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPCancelDLPJobOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPCancelDLPJobOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPCreateDeidentifyTemplateOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateDeidentifyTemplateOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPCreateDLPJobOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateDLPJobOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPCreateInspectTemplateOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateInspectTemplateOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPCreateJobTriggerOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateJobTriggerOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPCreateStoredInfoTypeOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateStoredInfoTypeOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPDeidentifyContentOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeidentifyContentOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPDeleteDeidentifyTemplateOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteDeidentifyTemplateOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPDeleteDlpJobOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteDlpJobOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPDeleteInspectTemplateOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteInspectTemplateOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPDeleteJobTriggerOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteJobTriggerOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPDeleteStoredInfoTypeOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteStoredInfoTypeOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPGetDeidentifyTemplateOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetDeidentifyTemplateOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPGetDlpJobOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetDlpJobOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPGetInspectTemplateOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetInspectTemplateOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPGetJobTripperOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetJobTripperOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPGetStoredInfoTypeOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetStoredInfoTypeOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPInspectContentOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPInspectContentOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPListDeidentifyTemplatesOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPListDeidentifyTemplatesOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPListDlpJobsOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPListDlpJobsOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPListInfoTypesOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPListInfoTypesOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPListInspectTemplatesOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPListInspectTemplatesOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPListJobTriggersOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPListJobTriggersOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPListStoredInfoTypesOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPListStoredInfoTypesOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPRedactImageOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPRedactImageOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPReidentifyContentOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPReidentifyContentOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPUpdateDeidentifyTemplateOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPUpdateDeidentifyTemplateOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPUpdateInspectTemplateOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPUpdateInspectTemplateOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPUpdateJobTriggerOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPUpdateJobTriggerOperator",
            ),
            (
                "airflow.gcp.operators.dlp.CloudDLPUpdateStoredInfoTypeOperator",
                "airflow.contrib.operators.gcp_dlp_operator.CloudDLPUpdateStoredInfoTypeOperator",
            ),
            (
                "airflow.gcp.operators.functions.GcfFunctionDeleteOperator",
                "airflow.contrib.operators.gcp_function_operator.GcfFunctionDeleteOperator",
            ),
            (
                "airflow.gcp.operators.functions.GcfFunctionDeployOperator",
                "airflow.contrib.operators.gcp_function_operator.GcfFunctionDeployOperator",
            ),
            (
                "airflow.gcp.operators.natural_language.CloudLanguageAnalyzeEntitiesOperator",
                "airflow.contrib.operators.gcp_natural_language_operator."
                "CloudLanguageAnalyzeEntitiesOperator",
            ),
            (
                "airflow.gcp.operators.natural_language.CloudLanguageAnalyzeEntitySentimentOperator",
                "airflow.contrib.operators.gcp_natural_language_operator."
                "CloudLanguageAnalyzeEntitySentimentOperator",
            ),
            (
                "airflow.gcp.operators.natural_language.CloudLanguageAnalyzeSentimentOperator",
                "airflow.contrib.operators.gcp_natural_language_operator."
                "CloudLanguageAnalyzeSentimentOperator",
            ),
            (
                "airflow.gcp.operators.natural_language.CloudLanguageClassifyTextOperator",
                "airflow.contrib.operators.gcp_natural_language_operator.CloudLanguageClassifyTextOperator",
            ),
            (
                "airflow.gcp.operators.spanner.CloudSpannerInstanceDatabaseDeleteOperator",
                "airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseDeleteOperator",
            ),
            (
                "airflow.gcp.operators.spanner.CloudSpannerInstanceDatabaseDeployOperator",
                "airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseDeployOperator",
            ),
            (
                "airflow.gcp.operators.spanner.CloudSpannerInstanceDatabaseQueryOperator",
                "airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseQueryOperator",
            ),
            (
                "airflow.gcp.operators.spanner.CloudSpannerInstanceDatabaseUpdateOperator",
                "airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseUpdateOperator",
            ),
            (
                "airflow.gcp.operators.spanner.CloudSpannerInstanceDeleteOperator",
                "airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDeleteOperator",
            ),
            (
                "airflow.gcp.operators.spanner.CloudSpannerInstanceDeployOperator",
                "airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDeployOperator",
            ),
            (
                "airflow.gcp.operators.speech_to_text.GcpSpeechToTextRecognizeSpeechOperator",
                "airflow.contrib.operators.gcp_speech_to_text_operator."
                "GcpSpeechToTextRecognizeSpeechOperator",
            ),
            (
                "airflow.gcp.operators.text_to_speech.GcpTextToSpeechSynthesizeOperator",
                "airflow.contrib.operators.gcp_text_to_speech_operator.GcpTextToSpeechSynthesizeOperator",
            ),
            (
                "airflow.gcp.operators.cloud_storage_transfer_service.GcpTransferServiceJobCreateOperator",
                "airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceJobCreateOperator",
            ),
            (
                "airflow.gcp.operators.cloud_storage_transfer_service.GcpTransferServiceJobDeleteOperator",
                "airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceJobDeleteOperator",
            ),
            (
                "airflow.gcp.operators.cloud_storage_transfer_service.GcpTransferServiceJobUpdateOperator",
                "airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceJobUpdateOperator",
            ),
            (
                "airflow.gcp.operators.cloud_storage_transfer_service."
                "GcpTransferServiceOperationCancelOperator",
                "airflow.contrib.operators.gcp_transfer_operator."
                "GcpTransferServiceOperationCancelOperator",
            ),
            (
                "airflow.gcp.operators.cloud_storage_transfer_service."
                "GcpTransferServiceOperationGetOperator",
                "airflow.contrib.operators.gcp_transfer_operator."
                "GcpTransferServiceOperationGetOperator",
            ),
            (
                "airflow.gcp.operators.cloud_storage_transfer_service."
                "GcpTransferServiceOperationPauseOperator",
                "airflow.contrib.operators.gcp_transfer_operator."
                "GcpTransferServiceOperationPauseOperator",
            ),
            (
                "airflow.gcp.operators.cloud_storage_transfer_service."
                "GcpTransferServiceOperationResumeOperator",
                "airflow.contrib.operators.gcp_transfer_operator."
                "GcpTransferServiceOperationResumeOperator",
            ),
            (
                "airflow.gcp.operators.cloud_storage_transfer_service."
                "GcpTransferServiceOperationsListOperator",
                "airflow.contrib.operators.gcp_transfer_operator."
                "GcpTransferServiceOperationsListOperator",
            ),
            (
                "airflow.gcp.operators.cloud_storage_transfer_service."
                "GoogleCloudStorageToGoogleCloudStorageTransferOperator",
                "airflow.contrib.operators.gcp_transfer_operator."
                "GoogleCloudStorageToGoogleCloudStorageTransferOperator",
            ),
            (
                "airflow.gcp.operators.translate.CloudTranslateTextOperator",
                "airflow.contrib.operators.gcp_translate_operator.CloudTranslateTextOperator",
            ),
            (
                "airflow.gcp.operators.translate_speech.GcpTranslateSpeechOperator",
                "airflow.contrib.operators.gcp_translate_speech_operator.GcpTranslateSpeechOperator",
            ),
            (
                "airflow.gcp.operators.video_intelligence."
                "CloudVideoIntelligenceDetectVideoExplicitContentOperator",
                "airflow.contrib.operators.gcp_video_intelligence_operator."
                "CloudVideoIntelligenceDetectVideoExplicitContentOperator",
            ),
            (
                "airflow.gcp.operators.video_intelligence."
                "CloudVideoIntelligenceDetectVideoLabelsOperator",
                "airflow.contrib.operators.gcp_video_intelligence_operator."
                "CloudVideoIntelligenceDetectVideoLabelsOperator",
            ),
            (
                "airflow.gcp.operators.video_intelligence."
                "CloudVideoIntelligenceDetectVideoShotsOperator",
                "airflow.contrib.operators.gcp_video_intelligence_operator."
                "CloudVideoIntelligenceDetectVideoShotsOperator",
            ),
            (
                "airflow.gcp.operators.vision.CloudVisionAddProductToProductSetOperator",
                "airflow.contrib.operators.gcp_vision_operator.CloudVisionAddProductToProductSetOperator",
            ),
            (
                "airflow.gcp.operators.vision.CloudVisionAnnotateImageOperator",
                "airflow.contrib.operators.gcp_vision_operator.CloudVisionAnnotateImageOperator",
            ),
            (
                "airflow.gcp.operators.vision.CloudVisionDetectDocumentTextOperator",
                "airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectDocumentTextOperator",
            ),
            (
                "airflow.gcp.operators.vision.CloudVisionDetectImageLabelsOperator",
                "airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectImageLabelsOperator",
            ),
            (
                "airflow.gcp.operators.vision.CloudVisionDetectImageSafeSearchOperator",
                "airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectImageSafeSearchOperator",
            ),
            (
                "airflow.gcp.operators.vision.CloudVisionDetectTextOperator",
                "airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectTextOperator",
            ),
            (
                "airflow.gcp.operators.vision.CloudVisionProductCreateOperator",
                "airflow.contrib.operators.gcp_vision_operator.CloudVisionProductCreateOperator",
            ),
            (
                "airflow.gcp.operators.vision.CloudVisionProductDeleteOperator",
                "airflow.contrib.operators.gcp_vision_operator.CloudVisionProductDeleteOperator",
            ),
            (
                "airflow.gcp.operators.vision.CloudVisionProductGetOperator",
                "airflow.contrib.operators.gcp_vision_operator.CloudVisionProductGetOperator",
            ),
            (
                "airflow.gcp.operators.vision.CloudVisionProductSetCreateOperator",
                "airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetCreateOperator",
            ),
            (
                "airflow.gcp.operators.vision.CloudVisionProductSetDeleteOperator",
                "airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetDeleteOperator",
            ),
            (
                "airflow.gcp.operators.vision.CloudVisionProductSetGetOperator",
                "airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetGetOperator",
            ),
            (
                "airflow.gcp.operators.vision.CloudVisionProductSetUpdateOperator",
                "airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetUpdateOperator",
            ),
            (
                "airflow.gcp.operators.vision.CloudVisionProductUpdateOperator",
                "airflow.contrib.operators.gcp_vision_operator.CloudVisionProductUpdateOperator",
            ),
            (
                "airflow.gcp.operators.vision.CloudVisionReferenceImageCreateOperator",
                "airflow.contrib.operators.gcp_vision_operator.CloudVisionReferenceImageCreateOperator",
            ),
            (
                "airflow.gcp.operators.vision.CloudVisionRemoveProductFromProductSetOperator",
                "airflow.contrib.operators.gcp_vision_operator."
                "CloudVisionRemoveProductFromProductSetOperator",
            ),
            (
                "airflow.operators.gcs_to_bq.GoogleCloudStorageToBigQueryOperator",
                "airflow.contrib.operators.gcs_to_bq.GoogleCloudStorageToBigQueryOperator",
            ),
            (
                "airflow.operators.gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator",
                "airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator",
            ),
            (
                "airflow.operators.gcs_to_s3.GoogleCloudStorageToS3Operator",
                "airflow.contrib.operators.gcs_to_s3.GoogleCloudStorageToS3Operator",
            ),
            (
                "airflow.gcp.operators.mlengine.MLEngineBatchPredictionOperator",
                "airflow.contrib.operators.mlengine_operator.MLEngineBatchPredictionOperator",
            ),
            (
                "airflow.gcp.operators.mlengine.MLEngineModelOperator",
                "airflow.contrib.operators.mlengine_operator.MLEngineModelOperator",
            ),
            (
                "airflow.gcp.operators.mlengine.MLEngineTrainingOperator",
                "airflow.contrib.operators.mlengine_operator.MLEngineTrainingOperator",
            ),
            (
                "airflow.gcp.operators.mlengine.MLEngineVersionOperator",
                "airflow.contrib.operators.mlengine_operator.MLEngineVersionOperator",
            ),
            (
                "airflow.operators.mssql_to_gcs.MsSqlToGoogleCloudStorageOperator",
                "airflow.contrib.operators.mssql_to_gcs.MsSqlToGoogleCloudStorageOperator",
            ),
            (
                "airflow.operators.mysql_to_gcs.MySqlToGoogleCloudStorageOperator",
                "airflow.contrib.operators.mysql_to_gcs.MySqlToGoogleCloudStorageOperator",
            ),
            (
                "airflow.operators.postgres_to_gcs.PostgresToGoogleCloudStorageOperator",
                "airflow.contrib.operators.postgres_to_gcs_operator."
                "PostgresToGoogleCloudStorageOperator",
            ),
            (
                "airflow.gcp.operators.pubsub.PubSubPublishOperator",
                "airflow.contrib.operators.pubsub_operator.PubSubPublishOperator",
            ),
            (
                "airflow.gcp.operators.pubsub.PubSubSubscriptionCreateOperator",
                "airflow.contrib.operators.pubsub_operator.PubSubSubscriptionCreateOperator",
            ),
            (
                "airflow.gcp.operators.pubsub.PubSubSubscriptionDeleteOperator",
                "airflow.contrib.operators.pubsub_operator.PubSubSubscriptionDeleteOperator",
            ),
            (
                "airflow.gcp.operators.pubsub.PubSubTopicCreateOperator",
                "airflow.contrib.operators.pubsub_operator.PubSubTopicCreateOperator",
            ),
            (
                "airflow.gcp.operators.pubsub.PubSubTopicDeleteOperator",
                "airflow.contrib.operators.pubsub_operator.PubSubTopicDeleteOperator",
            ),
            (
                "airflow.operators.sql_to_gcs.BaseSQLToGoogleCloudStorageOperator",
                "airflow.contrib.operators.sql_to_gcs.BaseSQLToGoogleCloudStorageOperator",
            ),
            (
                "airflow.gcp.operators.dataproc.DataprocClusterCreateOperator",
                "airflow.contrib.operators.dataproc_operator.DataprocClusterCreateOperator",
            ),
            (
                "airflow.gcp.operators.dataproc.DataprocClusterDeleteOperator",
                "airflow.contrib.operators.dataproc_operator.DataprocClusterDeleteOperator",
            ),
            (
                "airflow.gcp.operators.dataproc.DataprocClusterScaleOperator",
                "airflow.contrib.operators.dataproc_operator.DataprocClusterScaleOperator",
            ),
            (
                "airflow.gcp.operators.dataproc.DataProcHadoopOperator",
                "airflow.contrib.operators.dataproc_operator.DataProcHadoopOperator",
            ),
            (
                "airflow.gcp.operators.dataproc.DataProcHiveOperator",
                "airflow.contrib.operators.dataproc_operator.DataProcHiveOperator",
            ),
            (
                "airflow.gcp.operators.dataproc.DataProcJobBaseOperator",
                "airflow.contrib.operators.dataproc_operator.DataProcJobBaseOperator",
            ),
            (
                "airflow.gcp.operators.dataproc.DataprocOperationBaseOperator",
                "airflow.contrib.operators.dataproc_operator.DataprocOperationBaseOperator",
            ),
            (
                "airflow.gcp.operators.dataproc.DataProcPigOperator",
                "airflow.contrib.operators.dataproc_operator.DataProcPigOperator",
            ),
            (
                "airflow.gcp.operators.dataproc.DataProcPySparkOperator",
                "airflow.contrib.operators.dataproc_operator.DataProcPySparkOperator",
            ),
            (
                "airflow.gcp.operators.dataproc.DataProcSparkOperator",
                "airflow.contrib.operators.dataproc_operator.DataProcSparkOperator",
            ),
            (
                "airflow.gcp.operators.dataproc.DataProcSparkSqlOperator",
                "airflow.contrib.operators.dataproc_operator.DataProcSparkSqlOperator",
            ),
            (
                "airflow.gcp.operators.dataproc.DataprocWorkflowTemplateInstantiateInlineOperator",
                "airflow.contrib.operators.dataproc_operator."
                "DataprocWorkflowTemplateInstantiateInlineOperator",
            ),
            (
                "airflow.gcp.operators.dataproc.DataprocWorkflowTemplateInstantiateOperator",
                "airflow.contrib.operators.dataproc_operator."
                "DataprocWorkflowTemplateInstantiateOperator",
            ),
        ]
    )
    def test_operators_paths(self, new_path: str, old_path: str):
        self.assert_proper_import(new_path, old_path)

    @parameterized.expand(
        [
            (
                "airflow.gcp.sensors.bigtable.BigtableTableWaitForReplicationSensor",
                "airflow.contrib.operators.gcp_bigtable_operator."
                "BigtableTableWaitForReplicationSensor",
            ),
            (
                "airflow.gcp.sensors.cloud_storage_transfer_service."
                "GCPTransferServiceWaitForJobStatusSensor",
                "airflow.contrib.sensors.gcp_transfer_sensor."
                "GCPTransferServiceWaitForJobStatusSensor",
            ),
            (
                "airflow.gcp.sensors.pubsub.PubSubPullSensor",
                "airflow.contrib.sensors.pubsub_sensor.PubSubPullSensor",
            ),
        ]
    )
    def test_sensor_paths(self, new_path: str, old_path: str):
        self.assert_proper_import(new_path, old_path)
