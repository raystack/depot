# Redis Sink

### Input Schema
Redis  sink in Depot only supports Protobuf and JSON schema as of now. Support for Avro format is planned for a future release of Depot.

### Data Types
Redis sink can be created in 2 different modes based on the value of [`SINK_REDIS_DATA_TYPE`](../reference/configuration/redis.md#sink_redis_data_type): HashSet or List
  - `Hashset`: For each message, an entry of the format `key : field : value` is generated and pushed to Redis. Field and value are generated on the basis of the config [`SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING`](../reference/configuration/redis.md#sink_redis_hashset_field_to_column_mapping)
  - `List`: For each message entry of the format `key : value` is generated and pushed to Redis. Value is fetched for the proto index provided in the config [`SINK_REDIS_LIST_DATA_PROTO_INDEX`](../reference/configuration/redis.md#sink_redis_list_data_proto_index)

The `key` is picked up from a field in the message itself.

Limitation: Depot Redis sink only supports Key-Value, HashSet and List entries as of now.

### Deployment Types
Redis sink, as of now, supports two different Deployment Types `Standalone` and `Cluster`. This can be configured in the Depot environment variable `SINK_REDIS_DEPLOYMENT_TYPE`. 
