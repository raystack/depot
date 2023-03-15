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