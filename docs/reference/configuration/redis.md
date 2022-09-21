# Redis

A Redis sink in Depot requires the following environment variables to be set along with Generic ones

### `SINK_REDIS_URLS`

REDIS server instance hostname/IP address followed by its port.

- Example value: `localhost:6379,localhost:6380`
- Type: `required`

### `SINK_REDIS_DATA_TYPE`

To select whether you want to push your data as a `KEYVALUE`, `HASHSET` or as a `LIST` data type.

- Example value: `Hashset`
- Type: `required`
- Default value: `Hashset`

### `SINK_REDIS_KEY_TEMPLATE`

The string that will act as the key for each Redis entry. This key can be configured as per the requirement, a constant or can extract value from each message and use that as the Redis key.

- Example value: `Service\_%%s,1`

  This will take the value with index 1 from proto and create the Redis keys as per the template.

- Type: `required`

### `SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING`

This is the field that decides what all data will be stored in the HashSet for each message.
- Example value: `{"order_number":"ORDER_NUMBER","event_timestamp":"TIMESTAMP"}`
- Type: `required (For Hashset)`

### `SINK_REDIS_KEY_VALUE_DATA_FIELD_NAME`

This field decides what data will be stored in the value part of key-value pair

- Example value: `customer_id`

  This will get the value of the field with name `customer_id` in your proto and push that to the Redis as value with the corresponding keyTemplate

- Type: `required (For KeyValue)`

### `SINK_REDIS_LIST_DATA_FIELD_NAME`

This field decides what all data will be stored in the List for each message.

- Example value: `customer_id`

  This will get the value of the field with name `customer_id` in your proto and push that to the Redis list with the corresponding keyTemplate

- Type: `required (For List)`

### `SINK_REDIS_TTL_TYPE`

- Example value: `DURATION`
- Type: `optional`
- Default value: `DISABLE`
- Choice of Redis TTL type.It can be:
    - `DURATION`: After which the Key will be expired and removed from Redis \(UNIT- seconds\)
    - `EXACT_TIME`: Precise UNIX timestamp after which the Key will be expired

### `SINK_REDIS_TTL_VALUE`

Redis TTL value in Unix Timestamp for `EXACT_TIME` TTL type, In Seconds for `DURATION` TTL type.

- Example value: `100000`
- Type: `optional`
- Default value: `0`

### `SINK_REDIS_DEPLOYMENT_TYPE`

The Redis deployment you are using. At present, we support `Standalone` and `Cluster` types.

- Example value: `Standalone`
- Type: `required`
- Default value: `Standalone`
