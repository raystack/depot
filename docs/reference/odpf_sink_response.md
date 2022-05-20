# OdpfSinkResponse
```java
public class OdpfSinkResponse {
  private final Map<Long, ErrorInfo> errors = new HashMap<>();
  ...
}

```
SinkResponse will be returned by odpfSink.pushToSink(messageList) function call.
The response contains error map indexed by message in the input list.

## Errors
These errors are returned by sinks in the OdpfSinkResponse object. The error type are:

* DESERIALIZATION_ERROR
* INVALID_MESSAGE_ERROR
* UNKNOWN_FIELDS_ERROR
* SINK_4XX_ERROR
* SINK_5XX_ERROR
* SINK_UNKNOWN_ERROR
* DEFAULT_ERROR
    * If no error is specified

