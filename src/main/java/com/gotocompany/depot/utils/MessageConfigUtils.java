package com.gotocompany.depot.utils;

import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.common.Tuple;

public class MessageConfigUtils {

    public static Tuple<SinkConnectorSchemaMessageMode, String> getModeAndSchema(SinkConfig sinkConfig) {
        SinkConnectorSchemaMessageMode mode = sinkConfig.getSinkConnectorSchemaMessageMode();
        String schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? sinkConfig.getSinkConnectorSchemaProtoMessageClass() : sinkConfig.getSinkConnectorSchemaProtoKeyClass();
        return new Tuple<>(mode, schemaClass);
    }
}
