package com.gotocompany.depot.config;

import com.gotocompany.depot.config.converter.EmptyStringToNull;
import com.gotocompany.depot.config.converter.JsonToPropertiesConverter;
import com.gotocompany.depot.config.converter.RedisSinkDataTypeConverter;
import com.gotocompany.depot.config.converter.RedisSinkDeploymentTypeConverter;
import com.gotocompany.depot.config.converter.RedisSinkTtlTypeConverter;
import com.gotocompany.depot.config.preprocessor.Trim;
import com.gotocompany.depot.redis.enums.RedisSinkDataType;
import com.gotocompany.depot.redis.enums.RedisSinkDeploymentType;
import com.gotocompany.depot.redis.enums.RedisSinkTtlType;
import org.aeonbits.owner.Config;

import java.util.Properties;


@Config.DisableFeature(Config.DisableableFeature.PARAMETER_FORMATTING)
@Config.PreprocessorClasses({Trim.class})
public interface RedisSinkConfig extends SinkConfig {
    @Key("SINK_REDIS_URLS")
    String getSinkRedisUrls();

    @Key("SINK_REDIS_CLUSTER_MAX_ATTEMPTS")
    @DefaultValue("5")
    int getSinkRedisMaxAttempts();

    @Key("SINK_REDIS_AUTH_USERNAME")
    @ConverterClass(EmptyStringToNull.class)
    String getSinkRedisAuthUsername();

    @Key("SINK_REDIS_AUTH_PASSWORD")
    @ConverterClass(EmptyStringToNull.class)
    String getSinkRedisAuthPassword();

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

    @Key("SINK_REDIS_KEY_VALUE_DATA_FIELD_NAME")
    String getSinkRedisKeyValueDataFieldName();

    @Key("SINK_REDIS_LIST_DATA_FIELD_NAME")
    String getSinkRedisListDataFieldName();

    @Key("SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING")
    @ConverterClass(JsonToPropertiesConverter.class)
    @DefaultValue("")
    Properties getSinkRedisHashsetFieldToColumnMapping();
}
