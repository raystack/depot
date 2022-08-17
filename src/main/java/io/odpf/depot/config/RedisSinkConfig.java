package io.odpf.depot.config;

import io.odpf.depot.config.converter.JsonToPropertiesConverter;
import io.odpf.depot.redis.converter.RedisSinkDataTypeConverter;
import io.odpf.depot.redis.converter.RedisSinkDeploymentTypeConverter;
import io.odpf.depot.redis.converter.RedisSinkTtlTypeConverter;
import io.odpf.depot.redis.enums.RedisSinkDataType;
import io.odpf.depot.redis.enums.RedisSinkDeploymentType;
import io.odpf.depot.redis.enums.RedisSinkTtlType;

import java.util.Properties;


public interface RedisSinkConfig extends OdpfSinkConfig {
    @Key("SINK_REDIS_URLS")
    String getSinkRedisUrls();

    @Key("SINK_REDIS_KEY_TEMPLATE")
    String getSinkRedisKeyTemplate();

    @Key("SINK_REDIS_DATA_TYPE")
    @DefaultValue("HASHSET")
    @ConverterClass(RedisSinkDataTypeConverter.class)
    RedisSinkDataType getSinkRedisDataType();

    @Key("SINK_REDIS_TTL_TYPE")
    @DefaultValue("DISABLE")
    @ConverterClass(RedisSinkTtlTypeConverter.class)
    RedisSinkTtlType getSinkRedisTtlType();

    @Key("SINK_REDIS_TTL_VALUE")
    @DefaultValue("0")
    long getSinkRedisTtlValue();

    @Key("SINK_REDIS_DEPLOYMENT_TYPE")
    @DefaultValue("Standalone")
    @ConverterClass(RedisSinkDeploymentTypeConverter.class)
    RedisSinkDeploymentType getSinkRedisDeploymentType();

    @Key("SINK_REDIS_LIST_DATA_PROTO_INDEX")
    String getSinkRedisListDataProtoIndex();

    @Key("SINK_REDIS_KEY_VALUE_DATA_PROTO_INDEX")
    String getSinkRedisKeyValueDataProtoIndex();

    @Key("SINK_REDIS_KEY_VALUE_DATA_FIELD_NAME")
    String getSinkRedisKeyValueDataFieldName();

    @Key("SINK_REDIS_LIST_DATA_FIELD_NAME")
    String getSinkRedisListDataFieldName();

    @Key("SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING")
    @ConverterClass(JsonToPropertiesConverter.class)
    @DefaultValue("")
    Properties getSinkRedisHashsetFieldToColumnMapping();
}
