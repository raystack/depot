# Redis Sink

### Data Types
Redis sink can be created in 3 different modes based on the value of [`SINK_REDIS_DATA_TYPE`](../reference/configuration/redis.md#sink_redis_data_type): HashSet, KeyValue or List
  - `Hashset`: For each message, an entry of the format `key : field : value` is generated and pushed to Redis. Field and value are generated on the basis of the config [`SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING`](../reference/configuration/redis.md#sink_redis_hashset_field_to_column_mapping)
  - `List`: For each message entry of the format `key : value` is generated and pushed to Redis. The value is fetched for the Proto field name provided in the config [`SINK_REDIS_LIST_DATA_FIELD_NAME`](../reference/configuration/redis.md#sink_redis_list_data_field_name)
  - `KeyValue`: For each message entry of the format `key : value` is generated and pushed to Redis. The value is fetched for the proto field name provided in the config [`SINK_REDIS_KEY_VALUE_DATA_FIELD_NAME`](../reference/configuration/redis.md#sink_redis_key_value_data_field_name)

The `key` is picked up from a field in the message itself.

Limitation: Depot Redis sink only supports Key-Value, HashSet and List entries as of now.

### Deployment Types
Redis sink, as of now, supports two different Deployment Types `Standalone` and `Cluster`. This can be configured in the Depot environment variable `SINK_REDIS_DEPLOYMENT_TYPE`. 
