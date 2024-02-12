# HTTP Sink

## Overview
Firehose [HTTP](https://en.wikipedia.org/wiki/Hypertext_Transfer_Protocol) sink allows users to read data from Kafka and write to an HTTP endpoint. it requires the following [variables](../sinks/http-sink.md#http-sink) to be set. You need to create your own HTTP endpoint so that the Firehose can send data to it.

## Supported methods

Firehose supports `PUT`,`POST`,`PATCH` and `DELETE` verbs in its HTTP sink. The method can be configured using [`SINK_HTTPV2_REQUEST_METHOD`](../sinks/http-sink.md#SINK_HTTPV2_request_method).

## Authentication

Firehose HTTP sink supports [OAuth](https://en.wikipedia.org/wiki/OAuth) authentication. OAuth can be enabled for the HTTP sink by setting [`SINK_HTTPV2_OAUTH2_ENABLE`](../sinks/http-sink.md#SINK_HTTPV2_oauth2_enable)

```text
SINK_HTTPV2_OAUTH2_ACCESS_TOKEN_URL: https://sample-oauth.my-api.com/oauth2/token  # OAuth2 Token Endpoint.
SINK_HTTPV2_OAUTH2_CLIENT_NAME: client-name  # OAuth2 identifier issued to the client.
SINK_HTTPV2_OAUTH2_CLIENT_SECRET: client-secret # OAuth2 secret issued for the client.
SINK_HTTPV2_OAUTH2_SCOPE: User:read, sys:info  # Space-delimited scope overrides.
```

## Retries

Firehose allows for retrying to sink messages in case of failure of HTTP service. The HTTP error code ranges to retry can be configured with [`SINK_HTTPV2_RETRY_STATUS_CODE_RANGES`](../sinks/http-sink.md#SINK_HTTPV2_retry_status_code_ranges). HTTP request timeout can be configured with [`SINK_HTTPV2_REQUEST_TIMEOUT_MS`](../sinks/http-sink.md#SINK_HTTPV2_request_timeout_ms)


## Templating

Firehose HTTP sink supports payload templating using [`SINK_HTTPV2_JSON_BODY_TEMPLATE`](../sinks/http-sink.md#SINK_HTTPV2_json_body_template) configuration.  The template works only when the output data format [`SINK_HTTPV2_DATA_FORMAT`](../sinks/http-sink.md#SINK_HTTPV2_data_format) is JSON.

The JSON body template should be a valid JSON itself. It can of any JSON data type like integer, boolean, float, object, array or string.


### Constants (i.e. no template parameters)

Constant values of all data types, i.e. primitive, string, array, object are all supported in the JSON body template.

Examples Templates- 

* `SINK_HTTPV2_JSON_BODY_TEMPLATE=4.5601`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE=45601`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE=432423423556`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="text"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE=true`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE=[23,true,"tdff"]`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE={"err":23,"wee":true}`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE={}`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE=[]`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE=""`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE=null`


Corresponding payloads-

* `4.5601`
* `45601`
* `432423423556`
* `"text"`
* `true`
* `[23,true,"tdff"]`
* `{"err":23,"wee":true}`
* `{}`
* `[]`
* `""`
* `null`


### Primitive data types


All JSON primitive data types are supported, i.e. boolean, integer,long, float. The template will be replaced by the actual data types of the proto, i.e. the parsed template will not be a string. It will be of the type of the Proto field which was passed in the template.

Examples Templates-

* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%s,float_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%s,bool_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%s,int_value"`

Corresponding payloads-
* `4.5601`
* `true`
* `45601`


But if you want the parsed payload to be converted to a string instead of the primitive type then you'll have to follow the below example format -

Examples Templates-

* `SINK_HTTPV2_JSON_BODY_TEMPLATE="\"%s\",float_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="\"%s\",bool_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="\"%s\",int_value"`

Corresponding payloads-
* `"4.5601"`
* `"true"`
* `"45601"`



If you provide multiple primitive arguments in the template, then the parsed payload will become a string type

Examples Templates-

* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%s%s,float_value,int_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%s%s,bool_value,int_value"`


Corresponding payloads-
* `"4.560145601"`
* `"true45601"`


You can provide nested primitive fields inside a message proto field in the template arguments.

Examples Templates-

* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%s,message_value.int_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%s,message_value.bool_value"`


Corresponding payloads-
* `4145601`
* `true`


### String data type

JSON String data type is supported by providing a string proto field in the template arguments

Examples Templates-

* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%s,string_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%s%s,string_value,string_value"`


Corresponding payloads-
* `"dsfweg"`
* `"dsfwegdsfweg"`

Also you can append a constant string to the string proto field template argument

Examples Templates-

* `SINK_HTTPV2_JSON_BODY_TEMPLATE="sss %saa,string_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%sa a%s,string_value,string_value"`


Corresponding payloads-
* `"sss dsfwegaa"`
* `"dsfwega adsfweg"`

If you want to convert a primitive/object/array proto field to a string then you'll have to follow the below example format -

Examples Templates-

* `SINK_HTTPV2_JSON_BODY_TEMPLATE="\"%s\",float_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="\"%s\",bool_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="\"%s\",int_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="\"%s\",message_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="\"%s\",list_value"`

Corresponding payloads-
* `"4.5601"`
* `"true"`
* `"45601"`
* `"{\"ss\":23,\"ww\":true}"`
* `"[\"wwf\",33,true]"`

You can provide nested string fields inside a message proto field in the template arguments.

Examples Templates-

* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%s,message_value.string_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="\"%s\",message_value.bool_value"`


Corresponding payloads-
* `"daegaegaesg"`
* `"true"`


### Object/Message data type

You can pass a message type field in the arguments to parse it into a JSON Object

Examples Templates-

* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%s,message_value"`

Corresponding payloads-
* `{"ss":23,"ww":true}`

You can construct a JSON Object combing various proto fields in the template arguments

Examples Templates-

* `SINK_HTTPV2_JSON_BODY_TEMPLATE={"ss":"%s,float_value","ww":"%s,bool_value"}`

Corresponding payloads-
* `{"ss":23.221,"ww":false}`

You can pass template arguments of any data type in the keys of the JSON Object. But it will always be converted to string (only applies to the keys of the JSON Object).

Examples Templates-

* `SINK_HTTPV2_JSON_BODY_TEMPLATE={"%s,float_value":"%s,float_value","%s,bool_value":"%s,bool_value"}`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE={"%s,string_value":"%s,float_value"}`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE={"%s,message_value":23234.444}`

Corresponding payloads-
* `{"23.221":23.221,"false":false}`
* `{"sfddadz":23.221}`
* `{"{\"ss\":23,\"ww\":true}":23234.444}`

You can convert a template argument of message data type to a JSON string by using below format-

Examples Templates-

* `SINK_HTTPV2_JSON_BODY_TEMPLATE="\"%s\",message_value"`

Corresponding payloads-
* `"{\"ss\":23,\"ww\":true}"`

You can provide nested message type proto fields inside a message proto field in the template arguments.

Examples Templates-

* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%s,message_value.nested_message_value"`

Corresponding payloads-
* `{"ss":23.221,"ww":false}`



### Array/Repeated data type

You can pass a repeated proto field in the template arguments, which will get parsed into a JSON array type.

Examples Templates -

* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%s,list_value"`

Corresponding payloads -
* `["wfwf",true,222]`

You can convert a template argument of repeated data type to a JSON string by using below format-

Examples Templates-

* `SINK_HTTPV2_JSON_BODY_TEMPLATE="\"%s\",list_value"`

Corresponding payloads-
* `"[\"wwf\",33,true]"`

You can construct a JSON array combing various proto fields in the template arguments

Examples Templates-

* `SINK_HTTPV2_JSON_BODY_TEMPLATE=["ss","%s,float_value","%s,bool_value"]`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE=["ss","%s,message_value"]`

Corresponding payloads-
* `["ss",23.221,false]`
* `["ss",{"swws":23.221,"ww":false}]`

You can provide nested message type proto fields inside a message proto field in the template arguments.

Examples Templates-

* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%s,message_value.list_value"`

Corresponding payloads-

* `["wfwf",true,222]`

You can extract an element from a particular index from the repeated proto field.

Examples Templates-

* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%s,list_value[4]"`

Corresponding payloads-
* `3325`










