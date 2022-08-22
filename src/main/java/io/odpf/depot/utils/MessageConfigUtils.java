package io.odpf.depot.utils;

import io.odpf.depot.common.Tuple;
import io.odpf.depot.config.OdpfSinkConfig;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;

public class MessageConfigUtils {

    public static Tuple<SinkConnectorSchemaMessageMode, String> getModeAndSchema(OdpfSinkConfig sinkConfig) {
        SinkConnectorSchemaMessageMode mode = sinkConfig.getSinkConnectorSchemaMessageMode();
        String schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? sinkConfig.getSinkConnectorSchemaProtoMessageClass() : sinkConfig.getSinkConnectorSchemaProtoKeyClass();
        return new Tuple<>(mode, schemaClass);
    }
}
