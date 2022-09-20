# Redis Sink

### Input Schema
Redis  sink in Depot only supports Protobuf schema as of now. Support for JSON and Avro format is planned for a future release of Depot.

### Data Types
Redis sink can be created in 2 different modes based on the value of [`SINK_REDIS_DATA_TYPE`](../sinks/redis-sink.md#sink_redis_data_type): HashSet or List
  - `Hashset`: For each message, an entry of the format `key : field : value` is generated and pushed to Redis. field and value are generated on the basis of the config [`INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING`](../sinks/redis-sink.md#-input_schema_proto_to_column_mapping-2)
  - `List`: For each message entry of the format `key : value` is generated and pushed to Redis. Value is fetched for the proto index provided in the config [`SINK_REDIS_LIST_DATA_PROTO_INDEX`](../sinks/redis-sink.md#sink_redis_list_data_proto_index)

The `key` is picked up from a field in the message itself.

Limitation: Depot Redis sink only supports HashSet and List entries as of now.

### Deployment Types
Redis sink, as of now, supports two different Deployment Types `Standalone` and `Cluster`. This can be configured in the Depot environment variable `SINK_REDIS_DEPLOYMENT_TYPE`. 
