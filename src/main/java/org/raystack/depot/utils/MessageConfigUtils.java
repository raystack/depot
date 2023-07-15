package org.raystack.depot.utils;

import org.raystack.depot.config.SinkConfig;
import org.raystack.depot.message.SinkConnectorSchemaMessageMode;
import org.raystack.depot.common.Tuple;

public class MessageConfigUtils {

    public static Tuple<SinkConnectorSchemaMessageMode, String> getModeAndSchema(SinkConfig sinkConfig) {
        SinkConnectorSchemaMessageMode mode = sinkConfig.getSinkConnectorSchemaMessageMode();
        String schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? sinkConfig.getSinkConnectorSchemaProtoMessageClass()
                : sinkConfig.getSinkConnectorSchemaProtoKeyClass();
        return new Tuple<>(mode, schemaClass);
    }
}
