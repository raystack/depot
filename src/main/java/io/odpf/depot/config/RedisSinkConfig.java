package io.odpf.depot.config;

import io.odpf.depot.redis.converter.RedisSinkDataTypeConverter;
import io.odpf.depot.redis.converter.RedisSinkDeploymentTypeConverter;
import io.odpf.depot.redis.converter.RedisSinkTtlTypeConverter;
import io.odpf.depot.redis.enums.RedisSinkDataType;
import io.odpf.depot.redis.enums.RedisSinkDeploymentType;
import io.odpf.depot.redis.enums.RedisSinkTtlType;
import org.aeonbits.owner.Config;


public interface RedisSinkConfig extends OdpfSinkConfig {
    @Config.Key("SINK_REDIS_URLS")
    String getSinkRedisUrls();

    @Config.Key("SINK_REDIS_KEY_TEMPLATE")
    String getSinkRedisKeyTemplate();

    @Config.Key("SINK_REDIS_DATA_TYPE")
    @Config.DefaultValue("HASHSET")
    @Config.ConverterClass(RedisSinkDataTypeConverter.class)
    RedisSinkDataType getSinkRedisDataType();

    @Config.Key("SINK_REDIS_TTL_TYPE")
    @Config.DefaultValue("DISABLE")
    @Config.ConverterClass(RedisSinkTtlTypeConverter.class)
    RedisSinkTtlType getSinkRedisTtlType();

    @Config.Key("SINK_REDIS_TTL_VALUE")
    @Config.DefaultValue("0")
    long getSinkRedisTtlValue();

    @Config.Key("SINK_REDIS_DEPLOYMENT_TYPE")
    @Config.DefaultValue("Standalone")
    @Config.ConverterClass(RedisSinkDeploymentTypeConverter.class)
    RedisSinkDeploymentType getSinkRedisDeploymentType();

    @Config.Key("SINK_REDIS_LIST_DATA_PROTO_INDEX")
    String getSinkRedisListDataProtoIndex();

    @Config.Key("SINK_REDIS_KEY_VALUE_DATA_PROTO_INDEX")
    String getSinkRedisKeyValuetDataProtoIndex();


}
