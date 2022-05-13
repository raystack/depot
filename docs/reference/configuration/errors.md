# Errors

These errors are returned by sinks. One can configure to which errors should be processed by which decorator. The error
type are:

* DESERIALIZATION_ERROR
* INVALID_MESSAGE_ERROR
* UNKNOWN_FIELDS_ERROR
* SINK_4XX_ERROR
* SINK_5XX_ERROR
* SINK_UNKNOWN_ERROR
* DEFAULT_ERROR 
  * If no error is specified