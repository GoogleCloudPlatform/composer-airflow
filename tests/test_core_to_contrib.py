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

import importlib
from typing import Any
from unittest import TestCase, mock

from parameterized import parameterized

HOOK = [
    (
        "airflow.gcp.hooks.compute.ComputeEngineHook",
        "airflow.contrib.hooks.gcp_compute_hook.GceHook",
    ),
    (
        "airflow.gcp.hooks.base.CloudBaseHook",
        "airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook",
    ),
    (
        "airflow.gcp.hooks.dataflow.DataflowHook",
        "airflow.contrib.hooks.gcp_dataflow_hook.DataFlowHook",
    ),
    (
        "airflow.providers.google.cloud.hooks.dataproc.DataprocHook",
        "airflow.contrib.hooks.gcp_dataproc_hook.DataProcHook",
    ),
    (
        "airflow.gcp.hooks.dlp.CloudDLPHook",
        "airflow.contrib.hooks.gcp_dlp_hook.CloudDLPHook",
    ),
    (
        "airflow.gcp.hooks.functions.CloudFunctionsHook",
        "airflow.contrib.hooks.gcp_function_hook.GcfHook",
    ),
    (
        "airflow.gcp.hooks.kms.CloudKMSHook",
        "airflow.contrib.hooks.gcp_kms_hook.GoogleCloudKMSHook",
    ),
    (
        "airflow.gcp.hooks.mlengine.MLEngineHook",
        "airflow.contrib.hooks.gcp_mlengine_hook.MLEngineHook",
    ),
    (
        "airflow.gcp.hooks.spanner.SpannerHook",
        "airflow.contrib.hooks.gcp_spanner_hook.CloudSpannerHook",
    ),
    (
        "airflow.gcp.hooks.speech_to_text.CloudSpeechToTextHook",
        "airflow.contrib.hooks.gcp_speech_to_text_hook.GCPSpeechToTextHook",
    ),
    (
        "airflow.gcp.hooks.text_to_speech.CloudTextToSpeechHook",
        "airflow.contrib.hooks.gcp_text_to_speech_hook.GCPTextToSpeechHook",
    ),
    (
        "airflow.gcp.hooks.gcs.GoogleCloudStorageHook",
        "airflow.contrib.hooks.gcs_hook.GoogleCloudStorageHook",
    ),
    (
        "airflow.gcp.hooks.cloud_build.CloudBuildHook",
        "airflow.contrib.hooks.gcp_cloud_build_hook.CloudBuildHook",
    ),
    (
        "airflow.gcp.hooks.bigtable.BigtableHook",
        "airflow.contrib.hooks.gcp_bigtable_hook.BigtableHook",
    ),
    (
        "airflow.gcp.hooks.kubernetes_engine.GKEClusterHook",
        "airflow.contrib.hooks.gcp_container_hook.GKEClusterHook",
    ),
    (
        "airflow.gcp.hooks.datastore.DatastoreHook",
        "airflow.contrib.hooks.datastore_hook.DatastoreHook",
    ),
    (
        "airflow.providers.google.cloud.hooks.natural_language.CloudNaturalLanguageHook",
        "airflow.contrib.hooks.gcp_natural_language_hook.CloudNaturalLanguageHook",
    ),
    (
        "airflow.providers.google.cloud.hooks.pubsub.PubSubHook",
        "airflow.contrib.hooks.gcp_pubsub_hook.PubSubHook",
    ),
    (
        "airflow.gcp.hooks.cloud_sql.CloudSQLHook",
        "airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook",
    ),
    (
        "airflow.gcp.hooks.cloud_sql.CloudSQLDatabaseHook",
        "airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook",
    ),
    (
        "airflow.gcp.hooks.tasks.CloudTasksHook",
        "airflow.contrib.hooks.gcp_tasks_hook.CloudTasksHook",
    ),
    (
        "airflow.gcp.hooks.cloud_storage_transfer_service.CloudDataTransferServiceHook",
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
        "airflow.providers.google.cloud.hooks.vision.CloudVisionHook",
        "airflow.contrib.hooks.gcp_vision_hook.CloudVisionHook",
    ),
    (
        "airflow.gcp.hooks.bigquery.BigQueryHook",
        "airflow.contrib.hooks.bigquery_hook.BigQueryHook",
    ),
    (
        "airflow.providers.amazon.aws.hooks.athena.AWSAthenaHook",
        "airflow.contrib.hooks.aws_athena_hook.AWSAthenaHook",
    ),
    (
        "airflow.providers.amazon.aws.hooks.datasync.AWSDataSyncHook",
        "airflow.contrib.hooks.aws_datasync_hook.AWSDataSyncHook",
    ),
    (
        "airflow.providers.amazon.aws.hooks.s3.S3Hook",
        "airflow.hooks.S3_hook.S3Hook",
    ),
    (
        "airflow.providers.amazon.aws.hooks.sqs.SQSHook",
        "airflow.contrib.hooks.aws_sqs_hook.SQSHook",
    ),
    (
        "airflow.providers.amazon.aws.hooks.lambda_function.AwsLambdaHook",
        "airflow.contrib.hooks.aws_lambda_hook.AwsLambdaHook",
    ),
    (
        "airflow.providers.amazon.aws.hooks.sns.AwsSnsHook",
        "airflow.contrib.hooks.aws_sns_hook.AwsSnsHook",
    ),
]

OPERATOR = [
    (
        "airflow.operators.adls_to_gcs.AdlsToGoogleCloudStorageOperator",
        "airflow.contrib.operators.adls_to_gcs.AdlsToGoogleCloudStorageOperator",
    ),
    (
        "airflow.gcp.operators.dataflow.DataflowCreateJavaJobOperator",
        "airflow.contrib.operators.dataflow_operator.DataFlowJavaOperator",
    ),
    (
        "airflow.gcp.operators.dataflow.DataflowCreatePythonJobOperator",
        "airflow.contrib.operators.dataflow_operator.DataFlowPythonOperator",
    ),
    (
        "airflow.gcp.operators.dataflow.DataflowTemplatedJobStartOperator",
        "airflow.contrib.operators.dataflow_operator.DataflowTemplateOperator",
    ),
    (
        "airflow.gcp.operators.datastore.CloudDatastoreExportEntitiesOperator",
        "airflow.contrib.operators.datastore_export_operator.DatastoreExportOperator",
    ),
    (
        "airflow.gcp.operators.datastore.CloudDatastoreImportEntitiesOperator",
        "airflow.contrib.operators.datastore_import_operator.DatastoreImportOperator",
    ),
    (
        "airflow.operators.local_to_gcs.FileToGoogleCloudStorageOperator",
        "airflow.contrib.operators.file_to_gcs.FileToGoogleCloudStorageOperator",
    ),
    (
        "airflow.gcp.operators.bigtable.BigtableUpdateClusterOperator",
        "airflow.contrib.operators.gcp_bigtable_operator.BigtableClusterUpdateOperator",
    ),
    (
        "airflow.gcp.operators.bigtable.BigtableCreateInstanceOperator",
        "airflow.contrib.operators.gcp_bigtable_operator.BigtableInstanceCreateOperator",
    ),
    (
        "airflow.gcp.operators.bigtable.BigtableDeleteInstanceOperator",
        "airflow.contrib.operators.gcp_bigtable_operator.BigtableInstanceDeleteOperator",
    ),
    (
        "airflow.gcp.operators.bigtable.BigtableCreateTableOperator",
        "airflow.contrib.operators.gcp_bigtable_operator.BigtableTableCreateOperator",
    ),
    (
        "airflow.gcp.operators.bigtable.BigtableDeleteTableOperator",
        "airflow.contrib.operators.gcp_bigtable_operator.BigtableTableDeleteOperator",
    ),
    (
        "airflow.gcp.operators.cloud_build.CloudBuildCreateOperator",
        "airflow.contrib.operators.gcp_cloud_build_operator.CloudBuildCreateBuildOperator",
    ),
    (
        "airflow.gcp.operators.compute.ComputeEngineBaseOperator",
        "airflow.contrib.operators.gcp_compute_operator.GceBaseOperator",
    ),
    (
        "airflow.gcp.operators.compute.ComputeEngineInstanceGroupUpdateManagerTemplateOperator",
        "airflow.contrib.operators.gcp_compute_operator."
        "GceInstanceGroupManagerUpdateTemplateOperator",
    ),
    (
        "airflow.gcp.operators.compute.ComputeEngineStartInstanceOperator",
        "airflow.contrib.operators.gcp_compute_operator.GceInstanceStartOperator",
    ),
    (
        "airflow.gcp.operators.compute.ComputeEngineStopInstanceOperator",
        "airflow.contrib.operators.gcp_compute_operator.GceInstanceStopOperator",
    ),
    (
        "airflow.gcp.operators.compute.ComputeEngineCopyInstanceTemplateOperator",
        "airflow.contrib.operators.gcp_compute_operator.GceInstanceTemplateCopyOperator",
    ),
    (
        "airflow.gcp.operators.compute.ComputeEngineSetMachineTypeOperator",
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
        "airflow.gcp.operators.dlp.CloudDLPDeleteDLPJobOperator",
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
        "airflow.gcp.operators.dlp.CloudDLPGetDLPJobOperator",
        "airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetDlpJobOperator",
    ),
    (
        "airflow.gcp.operators.dlp.CloudDLPGetInspectTemplateOperator",
        "airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetInspectTemplateOperator",
    ),
    (
        "airflow.gcp.operators.dlp.CloudDLPGetDLPJobTriggerOperator",
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
        "airflow.gcp.operators.dlp.CloudDLPListDLPJobsOperator",
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
        "airflow.gcp.operators.functions.CloudFunctionDeleteFunctionOperator",
        "airflow.contrib.operators.gcp_function_operator.GcfFunctionDeleteOperator",
    ),
    (
        "airflow.gcp.operators.functions.CloudFunctionDeployFunctionOperator",
        "airflow.contrib.operators.gcp_function_operator.GcfFunctionDeployOperator",
    ),
    (
        "airflow.providers.google.cloud.operators.natural_language."
        "CloudNaturalLanguageAnalyzeEntitiesOperator",
        "airflow.contrib.operators.gcp_natural_language_operator."
        "CloudLanguageAnalyzeEntitiesOperator",
    ),
    (
        "airflow.providers.google.cloud.operators.natural_language."
        "CloudNaturalLanguageAnalyzeEntitySentimentOperator",
        "airflow.contrib.operators.gcp_natural_language_operator."
        "CloudLanguageAnalyzeEntitySentimentOperator",
    ),
    (
        "airflow.providers.google.cloud.operators.natural_language."
        "CloudNaturalLanguageAnalyzeSentimentOperator",
        "airflow.contrib.operators.gcp_natural_language_operator."
        "CloudLanguageAnalyzeSentimentOperator",
    ),
    (
        "airflow.providers.google.cloud.operators.natural_language."
        "CloudNaturalLanguageClassifyTextOperator",
        "airflow.contrib.operators.gcp_natural_language_operator.CloudLanguageClassifyTextOperator",
    ),
    (
        "airflow.gcp.operators.spanner.SpannerDeleteDatabaseInstanceOperator",
        "airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseDeleteOperator",
    ),
    (
        "airflow.gcp.operators.spanner.SpannerDeployDatabaseInstanceOperator",
        "airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseDeployOperator",
    ),
    (
        "airflow.gcp.operators.spanner.SpannerQueryDatabaseInstanceOperator",
        "airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseQueryOperator",
    ),
    (
        "airflow.gcp.operators.spanner.SpannerUpdateDatabaseInstanceOperator",
        "airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseUpdateOperator",
    ),
    (
        "airflow.gcp.operators.spanner.SpannerDeleteInstanceOperator",
        "airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDeleteOperator",
    ),
    (
        "airflow.gcp.operators.spanner.SpannerDeployInstanceOperator",
        "airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDeployOperator",
    ),
    (
        "airflow.gcp.operators.speech_to_text.CloudSpeechToTextRecognizeSpeechOperator",
        "airflow.contrib.operators.gcp_speech_to_text_operator.GcpSpeechToTextRecognizeSpeechOperator",
    ),
    (
        "airflow.gcp.operators.text_to_speech.CloudTextToSpeechSynthesizeOperator",
        "airflow.contrib.operators.gcp_text_to_speech_operator.GcpTextToSpeechSynthesizeOperator",
    ),
    (
        "airflow.gcp.operators.cloud_storage_transfer_service.CloudDataTransferServiceCreateJobOperator",
        "airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceJobCreateOperator",
    ),
    (
        "airflow.gcp.operators.cloud_storage_transfer_service.CloudDataTransferServiceDeleteJobOperator",
        "airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceJobDeleteOperator",
    ),
    (
        "airflow.gcp.operators.cloud_storage_transfer_service.CloudDataTransferServiceUpdateJobOperator",
        "airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceJobUpdateOperator",
    ),
    (
        "airflow.gcp.operators.cloud_storage_transfer_service."
        "CloudDataTransferServiceCancelOperationOperator",
        "airflow.contrib.operators.gcp_transfer_operator."
        "GcpTransferServiceOperationCancelOperator",
    ),
    (
        "airflow.gcp.operators.cloud_storage_transfer_service."
        "CloudDataTransferServiceGetOperationOperator",
        "airflow.contrib.operators.gcp_transfer_operator."
        "GcpTransferServiceOperationGetOperator",
    ),
    (
        "airflow.gcp.operators.cloud_storage_transfer_service."
        "CloudDataTransferServicePauseOperationOperator",
        "airflow.contrib.operators.gcp_transfer_operator."
        "GcpTransferServiceOperationPauseOperator",
    ),
    (
        "airflow.gcp.operators.cloud_storage_transfer_service."
        "CloudDataTransferServiceResumeOperationOperator",
        "airflow.contrib.operators.gcp_transfer_operator."
        "GcpTransferServiceOperationResumeOperator",
    ),
    (
        "airflow.gcp.operators.cloud_storage_transfer_service."
        "CloudDataTransferServiceListOperationsOperator",
        "airflow.contrib.operators.gcp_transfer_operator."
        "GcpTransferServiceOperationsListOperator",
    ),
    (
        "airflow.gcp.operators.cloud_storage_transfer_service."
        "CloudDataTransferServiceGCSToGCSOperator",
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
        "airflow.providers.google.cloud.operators.vision.CloudVisionAddProductToProductSetOperator",
        "airflow.contrib.operators.gcp_vision_operator.CloudVisionAddProductToProductSetOperator",
    ),
    (
        "airflow.providers.google.cloud.operators.vision.CloudVisionImageAnnotateOperator",
        "airflow.contrib.operators.gcp_vision_operator.CloudVisionAnnotateImageOperator",
    ),
    (
        "airflow.providers.google.cloud.operators.vision.CloudVisionTextDetectOperator",
        "airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectDocumentTextOperator",
    ),
    (
        "airflow.providers.google.cloud.operators.vision.CloudVisionDetectImageLabelsOperator",
        "airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectImageLabelsOperator",
    ),
    (
        "airflow.providers.google.cloud.operators.vision.CloudVisionDetectImageSafeSearchOperator",
        "airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectImageSafeSearchOperator",
    ),
    (
        "airflow.providers.google.cloud.operators.vision.CloudVisionDetectTextOperator",
        "airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectTextOperator",
    ),
    (
        "airflow.providers.google.cloud.operators.vision.CloudVisionCreateProductOperator",
        "airflow.contrib.operators.gcp_vision_operator.CloudVisionProductCreateOperator",
    ),
    (
        "airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductOperator",
        "airflow.contrib.operators.gcp_vision_operator.CloudVisionProductDeleteOperator",
    ),
    (
        "airflow.providers.google.cloud.operators.vision.CloudVisionGetProductOperator",
        "airflow.contrib.operators.gcp_vision_operator.CloudVisionProductGetOperator",
    ),
    (
        "airflow.providers.google.cloud.operators.vision.CloudVisionCreateProductSetOperator",
        "airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetCreateOperator",
    ),
    (
        "airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductSetOperator",
        "airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetDeleteOperator",
    ),
    (
        "airflow.providers.google.cloud.operators.vision.CloudVisionGetProductSetOperator",
        "airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetGetOperator",
    ),
    (
        "airflow.providers.google.cloud.operators.vision.CloudVisionUpdateProductSetOperator",
        "airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetUpdateOperator",
    ),
    (
        "airflow.providers.google.cloud.operators.vision.CloudVisionUpdateProductOperator",
        "airflow.contrib.operators.gcp_vision_operator.CloudVisionProductUpdateOperator",
    ),
    (
        "airflow.providers.google.cloud.operators.vision.CloudVisionCreateReferenceImageOperator",
        "airflow.contrib.operators.gcp_vision_operator.CloudVisionReferenceImageCreateOperator",
    ),
    (
        "airflow.providers.google.cloud.operators.vision.CloudVisionRemoveProductFromProductSetOperator",
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
        "airflow.operators.gcs_to_s3.GCSToS3Operator",
        "airflow.contrib.operators.gcs_to_s3.GoogleCloudStorageToS3Operator",
    ),
    (
        "airflow.gcp.operators.mlengine.MLEngineStartBatchPredictionJobOperator",
        "airflow.contrib.operators.mlengine_operator.MLEngineBatchPredictionOperator",
    ),
    (
        "airflow.gcp.operators.mlengine.MLEngineManageModelOperator",
        "airflow.contrib.operators.mlengine_operator.MLEngineModelOperator",
    ),
    (
        "airflow.gcp.operators.mlengine.MLEngineStartTrainingJobOperator",
        "airflow.contrib.operators.mlengine_operator.MLEngineTrainingOperator",
    ),
    (
        "airflow.gcp.operators.mlengine.MLEngineManageVersionOperator",
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
        "airflow.providers.google.cloud.operators.pubsub.PubSubPublishMessageOperator",
        "airflow.contrib.operators.pubsub_operator.PubSubPublishOperator",
    ),
    (
        "airflow.providers.google.cloud.operators.pubsub.PubSubCreateSubscriptionOperator",
        "airflow.contrib.operators.pubsub_operator.PubSubSubscriptionCreateOperator",
    ),
    (
        "airflow.providers.google.cloud.operators.pubsub.PubSubDeleteSubscriptionOperator",
        "airflow.contrib.operators.pubsub_operator.PubSubSubscriptionDeleteOperator",
    ),
    (
        "airflow.providers.google.cloud.operators.pubsub.PubSubCreateTopicOperator",
        "airflow.contrib.operators.pubsub_operator.PubSubTopicCreateOperator",
    ),
    (
        "airflow.providers.google.cloud.operators.pubsub.PubSubDeleteTopicOperator",
        "airflow.contrib.operators.pubsub_operator.PubSubTopicDeleteOperator",
    ),
    (
        "airflow.operators.sql_to_gcs.BaseSQLToGoogleCloudStorageOperator",
        "airflow.contrib.operators.sql_to_gcs.BaseSQLToGoogleCloudStorageOperator",
    ),
    (
        "airflow.providers.google.cloud."
        "operators.dataproc.DataprocClusterCreateOperator",
        "airflow.contrib.operators.dataproc_operator.DataprocClusterCreateOperator",
    ),
    (
        "airflow.providers.google.cloud."
        "operators.dataproc.DataprocClusterDeleteOperator",
        "airflow.contrib.operators.dataproc_operator.DataprocClusterDeleteOperator",
    ),
    (
        "airflow.providers.google.cloud."
        "operators.dataproc.DataprocClusterScaleOperator",
        "airflow.contrib.operators.dataproc_operator.DataprocClusterScaleOperator",
    ),
    (
        "airflow.providers.google.cloud."
        "operators.dataproc.DataProcHadoopOperator",
        "airflow.contrib.operators.dataproc_operator.DataProcHadoopOperator",
    ),
    (
        "airflow.providers.google.cloud."
        "operators.dataproc.DataProcHiveOperator",
        "airflow.contrib.operators.dataproc_operator.DataProcHiveOperator",
    ),
    (
        "airflow.providers.google.cloud."
        "operators.dataproc.DataProcJobBaseOperator",
        "airflow.contrib.operators.dataproc_operator.DataProcJobBaseOperator",
    ),
    (
        "airflow.providers.google.cloud."
        "operators.dataproc.DataProcPigOperator",
        "airflow.contrib.operators.dataproc_operator.DataProcPigOperator",
    ),
    (
        "airflow.providers.google.cloud."
        "operators.dataproc.DataProcPySparkOperator",
        "airflow.contrib.operators.dataproc_operator.DataProcPySparkOperator",
    ),
    (
        "airflow.providers.google.cloud."
        "operators.dataproc.DataProcSparkOperator",
        "airflow.contrib.operators.dataproc_operator.DataProcSparkOperator",
    ),
    (
        "airflow.providers.google.cloud."
        "operators.dataproc.DataProcSparkSqlOperator",
        "airflow.contrib.operators.dataproc_operator.DataProcSparkSqlOperator",
    ),
    (
        "airflow.providers.google.cloud."
        "operators.dataproc.DataprocWorkflowTemplateInstantiateInlineOperator",
        "airflow.contrib.operators.dataproc_operator."
        "DataprocWorkflowTemplateInstantiateInlineOperator",
    ),
    (
        "airflow.providers.google.cloud."
        "operators.dataproc.DataprocWorkflowTemplateInstantiateOperator",
        "airflow.contrib.operators.dataproc_operator."
        "DataprocWorkflowTemplateInstantiateOperator",
    ),
    (
        "airflow.gcp.operators.bigquery.BigQueryCheckOperator",
        "airflow.contrib.operators.bigquery_check_operator.BigQueryCheckOperator",
    ),
    (
        "airflow.gcp.operators.bigquery.BigQueryIntervalCheckOperator",
        "airflow.contrib.operators.bigquery_check_operator.BigQueryIntervalCheckOperator",
    ),
    (
        "airflow.gcp.operators.bigquery.BigQueryValueCheckOperator",
        "airflow.contrib.operators.bigquery_check_operator.BigQueryValueCheckOperator",
    ),
    (
        "airflow.gcp.operators.bigquery.BigQueryGetDataOperator",
        "airflow.contrib.operators.bigquery_get_data.BigQueryGetDataOperator",
    ),
    (
        "airflow.gcp.operators.bigquery.BigQueryExecuteQueryOperator",
        "airflow.contrib.operators.bigquery_operator.BigQueryOperator",
    ),
    (
        "airflow.gcp.operators.bigquery.BigQueryDeleteTableOperator",
        "airflow.contrib.operators.bigquery_table_delete_operator.BigQueryTableDeleteOperator",
    ),
    (
        "airflow.operators.bigquery_to_bigquery.BigQueryToBigQueryOperator",
        "airflow.contrib.operators.bigquery_to_bigquery.BigQueryToBigQueryOperator",
    ),
    (
        "airflow.operators.bigquery_to_gcs.BigQueryToCloudStorageOperator",
        "airflow.contrib.operators.bigquery_to_gcs.BigQueryToCloudStorageOperator",
    ),
    (
        "airflow.operators.bigquery_to_mysql.BigQueryToMySqlOperator",
        "airflow.contrib.operators.bigquery_to_mysql_operator.BigQueryToMySqlOperator",
    ),
    (
        "airflow.gcp.operators.gcs.GoogleCloudStorageBucketCreateAclEntryOperator",
        "airflow.contrib.operators.gcs_acl_operator.GoogleCloudStorageBucketCreateAclEntryOperator",
    ),
    (
        "airflow.gcp.operators.gcs.GoogleCloudStorageObjectCreateAclEntryOperator",
        "airflow.contrib.operators.gcs_acl_operator.GoogleCloudStorageObjectCreateAclEntryOperator",
    ),
    (
        "airflow.gcp.operators.gcs.GoogleCloudStorageDeleteOperator",
        "airflow.contrib.operators.gcs_delete_operator.GoogleCloudStorageDeleteOperator",
    ),
    (
        "airflow.gcp.operators.gcs.GoogleCloudStorageDownloadOperator",
        "airflow.contrib.operators.gcs_download_operator.GoogleCloudStorageDownloadOperator",
    ),
    (
        "airflow.gcp.operators.gcs.GoogleCloudStorageListOperator",
        "airflow.contrib.operators.gcs_list_operator.GoogleCloudStorageListOperator",
    ),
    (
        "airflow.gcp.operators.gcs.GoogleCloudStorageCreateBucketOperator",
        "airflow.contrib.operators.gcs_operator.GoogleCloudStorageCreateBucketOperator",
    ),
    (
        "airflow.providers.amazon.aws.operators.athena.AWSAthenaOperator",
        "airflow.contrib.operators.aws_athena_operator.AWSAthenaOperator",
    ),
    (
        "airflow.providers.amazon.aws.operators.batch.AwsBatchOperator",
        "airflow.contrib.operators.awsbatch_operator.AWSBatchOperator",
    ),
    (
        "airflow.providers.amazon.aws.operators.sqs.SQSPublishOperator",
        "airflow.contrib.operators.aws_sqs_publish_operator.SQSPublishOperator",
    ),
    (
        "airflow.providers.amazon.aws.operators.sns.SnsPublishOperator",
        "airflow.contrib.operators.sns_publish_operator.SnsPublishOperator",
    )
]

SENSOR = [
    (
        "airflow.gcp.sensors.bigtable.BigtableTableReplicationCompletedSensor",
        "airflow.contrib.operators.gcp_bigtable_operator."
        "BigtableTableWaitForReplicationSensor",
    ),
    (
        "airflow.gcp.sensors.cloud_storage_transfer_service."
        "CloudDataTransferServiceJobStatusSensor",
        "airflow.contrib.sensors.gcp_transfer_sensor."
        "GCPTransferServiceWaitForJobStatusSensor",
    ),
    (
        "airflow.providers.google.cloud.sensors.pubsub.PubSubPullSensor",
        "airflow.contrib.sensors.pubsub_sensor.PubSubPullSensor",
    ),
    (
        "airflow.gcp.sensors.bigquery.BigQueryTableExistenceSensor",
        "airflow.contrib.sensors.bigquery_sensor.BigQueryTableSensor",
    ),
    (
        "airflow.gcp.sensors.gcs.GoogleCloudStorageObjectSensor",
        "airflow.contrib.sensors.gcs_sensor.GoogleCloudStorageObjectSensor",
    ),
    (
        "airflow.gcp.sensors.gcs.GoogleCloudStorageObjectUpdatedSensor",
        "airflow.contrib.sensors.gcs_sensor.GoogleCloudStorageObjectUpdatedSensor",
    ),
    (
        "airflow.gcp.sensors.gcs.GoogleCloudStoragePrefixSensor",
        "airflow.contrib.sensors.gcs_sensor.GoogleCloudStoragePrefixSensor",
    ),
    (
        "airflow.gcp.sensors.gcs.GoogleCloudStorageUploadSessionCompleteSensor",
        "airflow.contrib.sensors.gcs_sensor.GoogleCloudStorageUploadSessionCompleteSensor",
    ),
    (
        "airflow.providers.amazon.aws.sensors.athena.AthenaSensor",
        "airflow.contrib.sensors.aws_athena_sensor.AthenaSensor",
    ),
    (
        "airflow.providers.amazon.aws.sensors.sqs.SQSSensor",
        "airflow.contrib.sensors.aws_sqs_sensor.SQSSensor",
    ),
]

PROTOCOLS = [
    (
        "airflow.providers.amazon.aws.hooks.batch_client.AwsBatchProtocol",
        "airflow.contrib.operators.awsbatch_operator.BatchProtocol",
    ),
]


ALL = HOOK + OPERATOR + SENSOR + PROTOCOLS

RENAMED_HOOKS = [
    (old_class, new_class)
    for old_class, new_class in HOOK + OPERATOR + SENSOR
    if old_class.rpartition(".")[2] != new_class.rpartition(".")[2]
]


class TestMovingCoreToContrib(TestCase):
    @staticmethod
    def assert_warning(msg: str, warning: Any):
        error = "Text '{}' not in warnings".format(msg)
        assert any(msg in str(w) for w in warning.warnings), error

    def assert_is_subclass(self, clazz, other):
        self.assertTrue(
            issubclass(clazz, other), "{} is not subclass of {}".format(clazz, other)
        )

    def assert_proper_import(self, old_resource, new_resource):
        new_path, _, _ = new_resource.rpartition(".")
        old_path, _, _ = old_resource.rpartition(".")
        with self.assertWarns(DeprecationWarning) as warning_msg:
            # Reload to see deprecation warning each time
            importlib.reload(importlib.import_module(old_path))
            self.assert_warning(new_path, warning_msg)

    @staticmethod
    def get_class_from_path(path_to_class):
        path, _, class_name = path_to_class.rpartition(".")
        module = importlib.import_module(path)
        class_ = getattr(module, class_name)
        return class_

    @parameterized.expand(PROTOCOLS)
    def test_is_protocol_deprecated(self, _, old_module):
        deprecation_warning_msg = "This class is deprecated."
        old_module_class = self.get_class_from_path(old_module)
        with self.assertWarnsRegex(DeprecationWarning, deprecation_warning_msg) as wrn:
            self.assertTrue(deprecation_warning_msg, wrn)
            old_module_class()

    @parameterized.expand(RENAMED_HOOKS)
    def test_is_class_deprecated(self, new_module, old_module):
        deprecation_warning_msg = "This class is deprecated."
        old_module_class = self.get_class_from_path(old_module)
        with self.assertWarnsRegex(DeprecationWarning, deprecation_warning_msg) as wrn:
            with mock.patch("{}.__init__".format(new_module)) as init_mock:
                init_mock.return_value = None
                self.assertTrue(deprecation_warning_msg, wrn)
                old_module_class()
                init_mock.assert_called_once_with()

    @parameterized.expand(ALL)
    def test_is_subclass(self, parent_class_path, sub_class_path):
        with mock.patch("{}.__init__".format(parent_class_path)):
            parent_class_path = self.get_class_from_path(parent_class_path)
            sub_class_path = self.get_class_from_path(sub_class_path)
            self.assert_is_subclass(sub_class_path, parent_class_path)

    @parameterized.expand(ALL)
    def test_warning_on_import(self, new_path, old_path):
        self.assert_proper_import(old_path, new_path)
