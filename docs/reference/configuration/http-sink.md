# HTTP Sink

An HTTP sink requires the following variables to be set along with Generic ones

## `SINK_HTTPV2_SERVICE_URL`


The HTTP endpoint of the service to which this consumer should PUT/POST/PATCH/DELETE data. This can be configured as per the requirement, a constant or a dynamic one \(which extract given field values from each message and use that as the endpoint\)
If service url is constant, messages will be sent as batches while in case of dynamic one each message will be sent as a separate request \(Since they’d be having different endpoints\).

- Example value: `http://http-service.test.io`
- Example value: `http://http-service.test.io/test-field/%s,order_number` This will take the value with field name `order_number` from proto and create the endpoint as per the template
- Type: `required`

## `SINK_HTTPV2_REQUEST_METHOD`

Defines the HTTP verb supported by the endpoint, Supports PUT, POST, PATCH and DELETE verbs as of now.

- Example value: `post`
- Type: `required`
- Default value: `put`

## `SINK_HTTPV2_HEADERS`

Defines the HTTP headers required to push the data to the above URL.

- Example value: `Authorization:auth_token, Accept:text/plain`
- Type: `optional`

## `SINK_HTTPV2_HEADERS_TEMPLATE`

Defines a template for creating  custom headers from the fields of a protobuf message. This should be a valid JSON itself.

* Example value: `{"%s,order_id":"Order"}`
* Type: `optional`

## `SINK_HTTPV2_HEADERS_PARAMETER_SOURCE`

Defines the source from which the fields should be parsed. This field should be present in order to use this feature.

- Example value: `Key`
- Type: `optional`
- Default value: `MESSAGE`

## `SINK_HTTPV2_QUERY_TEMPLATE`

Defines a template for creating a custom query from the fields of a protobuf message. This should be a valid JSON itself.

* Example value: `{"%s,order_id":"Order"}`
* Type: `optional`

## `SINK_HTTPV2_QUERY_PARAMETER_SOURCE`

Defines the source from which the fields should be parsed. This field should be present in order to use this feature.

- Example value: `Key`
- Type: `optional`
- Default value: `Message`

## `SINK_HTTPV2_REQUEST_MODE`

Defines Single or Batch request mode which corresponds to sending a single message payload or sending a batch payload of messages in the body of each http request.

* Example value: `BATCH`
* Type: `optional`
* Default value: `SINGLE`


## `SINK_HTTPV2_REQUEST_BODY_MODE`

Defines the type of format of the request body in the payload. This can be either of the following types-
* `RAW` - The raw Protobuf/ JSON encoded byte string containing both key and message
* `TEMPLATIZED_JSON` - JSON payload constructed by the provided JSON body template
* `JSON` - JSON payload containing all the fields of the Proto/JSON message
* `MESSAGE` - Only the Protobuf/ JSON encoded Message , excluding the key
  

* Example value: `TEMPLATIZED_JSON`
* Type: `optional`
* Default value: `RAW`


## `SINK_HTTPV2_REQUEST_LOG_STATUS_CODE_RANGES`

Defines the range of HTTP status codes for which request body will be logged. 

- Example value: `400-600`
- Type: `optional`
- Default value: `400-600`


## `SINK_HTTPV2_RETRY_STATUS_CODE_RANGES`

Defines the range of HTTP status codes for which retry will be attempted. Please remove 404 from retry code range in case of HTTP DELETE otherwise it might try to retry to delete already deleted resources.

- Example value: `400-600`
- Type: `optional`
- Default value: `400-600`

## `SINK_HTTPV2_JSON_BODY_TEMPLATE`


Defines a template for creating a custom request body from the fields of a protobuf message. This should be a valid JSON itself.

- Example value: `{"test":"%s,routes[0]", "%s,order_number" : "xxx"}`
- Type: `optional`

## `SINK_HTTPV2_DEFAULT_FIELD_VALUE_ENABLE`

Defines whether to send the default values in the request body for fields which are not present or null in the input Proto message

* Example value: `false`
* Type: `optional`
* Default value: `true`

## `SINK_HTTPV2_DELETE_BODY_ENABLE`

This config if set to true will allow body for the HTTP DELETE method, otherwise no payload will be sent with DELETE request.

- Example value: `false`
- Type: `optional`
- Default value: `true`


## `SINK_HTTPV2_MAX_CONNECTIONS`

Defines the maximum number of HTTP connections.

- Example value: `10`
- Type: `required`
- Default value: `10`

## `SINK_HTTPV2_REQUEST_TIMEOUT_MS`

Defines the connection timeout for the request in millis.

- Example value: `10000`
- Type: `required`
- Default value: `10000`

## `SINK_HTTPV2_OAUTH2_ENABLE`

Enable/Disable OAuth2 support for HTTP sink.

- Example value: `true`
- Type: `optional`
- Default value: `false`

## `SINK_HTTPV2_OAUTH2_ACCESS_TOKEN_URL`

Defines the OAuth2 Token Endpoint.

- Example value: `https://sample-oauth.my-api.com/oauth2/token`
- Type: `optional`

## `SINK_HTTPV2_OAUTH2_CLIENT_NAME`


Defines the OAuth2 identifier issued to the client.

- Example value: `client-name`
- Type: `optional`


## `SINK_HTTPV2_OAUTH2_CLIENT_SECRET`


Defines the OAuth2 secret issued for the client.

- Example value: `client-secret`
- Type: `optional`


## `SINK_HTTPV2_OAUTH2_SCOPE`


Space-delimited scope overrides. If scope override is not provided, no scopes will be granted to the token.

- Example value: `User:read, sys:info`
- Type: `optional`