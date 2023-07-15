package org.raystack.depot.utils;

import org.raystack.depot.common.Tuple;
import org.raystack.depot.config.RaystackSinkConfig;
import org.raystack.depot.message.SinkConnectorSchemaMessageMode;

public class MessageConfigUtils {

    public static Tuple<SinkConnectorSchemaMessageMode, String> getModeAndSchema(RaystackSinkConfig sinkConfig) {
        SinkConnectorSchemaMessageMode mode = sinkConfig.getSinkConnectorSchemaMessageMode();
        String schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? sinkConfig.getSinkConnectorSchemaProtoMessageClass()
                : sinkConfig.getSinkConnectorSchemaProtoKeyClass();
        return new Tuple<>(mode, schemaClass);
    }
}
