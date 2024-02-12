# Generic

All sinks require the following variables to be set

## `SINK_METRICS_APPLICATION_PREFIX`

Application prefix for sink metrics.

* Example value: `application_`
* Type: `required`

## `SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS`

Message log-message schema class

* Example value: `com.gotocompany.schema.MessageClass`
* Type: `required`

## `SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS`

Message log-key schema class

* Example value: `com.gotocompany.schema.KeyClass`
* Type: `required`

## `SINK_CONNECTOR_SCHEMA_DATA_TYPE`

Message raw data type

* Example value: `JSON`
* Type: `required`
* Default: `PROTOBUF`

## `SINK_CONNECTOR_SCHEMA_MESSAGE_MODE`

The type of raw message to read from

* Example value: `LOG_MESSAGE`
* Type: `required`
* Default: `LOG_MESSAGE`

## `SINK_CONNECTOR_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE`

Allow unknown fields in proto schema

* Example value: `true`
* Type: `required`
* Default: `false`

## `SINK_ADD_METADATA_ENABLED`

Defines whether to add Kafka metadata fields like topic, partition, offset, timestamp in the input proto messge.

* Example value: `true`
* Type: `optional`
* Default: `false`

## `SINK_METADATA_COLUMNS_TYPES`

Defines which Kafka metadata fields to add in the input proto message.
* Example value: `message_offset`
* Type: `optional`

## `SINK_DEFAULT_FIELD_VALUE_ENABLE`

Defines whether to send the default values in the request body for fields which are not present or null in the input Proto message

* Example value: `false`
* Type: `optional`
* Default value: `false`

## `METRIC_STATSD_HOST`

URL of the StatsD host

* Example value: `localhost`
* Type: `optional`
* Default value`: localhost`

## `METRIC_STATSD_PORT`

Port of the StatsD host

* Example value: `8125`
* Type: `optional`
* Default value`: 8125`

## `METRIC_STATSD_TAGS`

Global tags for StatsD metrics. Tags must be comma-separated.

* Example value: `team=engineering,app=myapp`
* Type: `optional`