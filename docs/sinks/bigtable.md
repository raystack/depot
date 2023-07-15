# Bigtable Sink

## Overview

Depot Bigtable Sink translates protobuf messages to bigtable records and insert them to a bigtable table. Its other responsibilities include validating the provided [column-family-schema](../reference/configuration/bigtable.md#sink_bigtable_column_family_mapping), and check whether the configured table exists in [Bigtable instance](../reference/configuration/bigtable.md#sink_bigtable_instance_id) or not.

Depot uses [Java Client Library for the Cloud Bigtable API](https://cloud.google.com/bigtable/docs/reference/libraries) to perform any operations on Bigtable.

## Setup Required

To be able to insert/update records in Bigtable, One must have following setup in place:

- [Bigtable Instance](../reference/configuration/bigtable.md#sink_bigtable_instance_id) belonging to the [GCP project](../reference/configuration/bigtable.md#sink_bigtable_google_cloud_project_id) provided in configuration
- Bigtable [Table](../reference/configuration/bigtable.md#sink_bigtable_table_id) where the records are supposed to be inserted/updated
- Column families that are provided as part of [column-family-mapping](../reference/configuration/bigtable.md#sink_bigtable_column_family_mapping)
- Google cloud [Bigtable IAM permission](https://cloud.google.com/bigtable/docs/access-control) required to access and modify the configured Bigtable Instance and Table

## Metrics

Check out the list of [metrics](../reference/metrics.md#bigtable-sink) captured under Bigtable Sink.

## Error Handling

[BigtableResponse](../../src/main/java/io/raystack/depot/bigtable/response/BigTableResponse.java) class have the list of failed [mutations](https://cloud.google.com/bigtable/docs/writes#write-types). [BigtableResponseParser](../../src/main/java/io/raystack/depot/bigtable/parser/BigTableResponseParser.java) looks for errors from each failed mutation and create [ErrorInfo](../../src/main/java/io/raystack/depot/error/ErrorInfo.java) objects based on the type/HttpStatusCode of the underlying error. This error info is then sent to the application.

| Error From Bigtable                 | Error Type Captured  |
| ----------------------------------- | -------------------- |
| Retryable Error                     | SINK_RETRYABLE_ERROR |
| Having status code in range 400-499 | SINK_4XX_ERROR       |
| Having status code in range 500-599 | SINK_5XX_ERROR       |
| Any other Error                     | SINK_UNKNOWN_ERROR   |

### Error Telemetry

[BigtableResponseParser](../../src/main/java/io/raystack/depot/bigtable/parser/BigTableResponseParser.java) looks for any specific error types sent from Bigtable and capture those under [BigtableTotalErrorMetrics](../reference/metrics.md#bigtable-sink) with suitable error tags.

| Error Type           | Error Tag Assigned   |
| -------------------- | -------------------- |
| Bad Request          | BAD_REQUEST          |
| Quota Failure        | QUOTA_FAILURE        |
| Precondition Failure | PRECONDITION_FAILURE |
| Any other Error      | RPC_FAILURE          |
