package org.raystack.depot.log;

import org.raystack.depot.RaystackSink;
import org.raystack.depot.RaystackSinkResponse;
import org.raystack.depot.config.RaystackSinkConfig;
import org.raystack.depot.error.ErrorInfo;
import org.raystack.depot.error.ErrorType;
import org.raystack.depot.exception.RaystackSinkException;
import org.raystack.depot.message.SinkConnectorSchemaMessageMode;
import org.raystack.depot.message.RaystackMessage;
import org.raystack.depot.message.RaystackMessageParser;
import org.raystack.depot.message.ParsedRaystackMessage;
import org.raystack.depot.metrics.Instrumentation;

import java.io.IOException;
import java.util.List;

public class LogSink implements RaystackSink {
    private final RaystackMessageParser raystackMessageParser;
    private final Instrumentation instrumentation;
    private final RaystackSinkConfig config;

    public LogSink(RaystackSinkConfig config, RaystackMessageParser raystackMessageParser,
            Instrumentation instrumentation) {
        this.raystackMessageParser = raystackMessageParser;
        this.instrumentation = instrumentation;
        this.config = config;
    }

    @Override
    public RaystackSinkResponse pushToSink(List<RaystackMessage> messages) throws RaystackSinkException {
        RaystackSinkResponse response = new RaystackSinkResponse();
        SinkConnectorSchemaMessageMode mode = config.getSinkConnectorSchemaMessageMode();
        String schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? config.getSinkConnectorSchemaProtoMessageClass()
                : config.getSinkConnectorSchemaProtoKeyClass();
        for (int ii = 0; ii < messages.size(); ii++) {
            RaystackMessage message = messages.get(ii);
            try {
                ParsedRaystackMessage parsedRaystackMessage = raystackMessageParser.parse(
                        message,
                        mode,
                        schemaClass);
                instrumentation.logInfo("\n================= DATA =======================\n{}"
                        + "\n================= METADATA =======================\n{}\n",
                        parsedRaystackMessage.toString(), message.getMetadataString());
            } catch (IOException e) {
                response.addErrors(ii, new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR));
            }
        }
        return response;
    }

    @Override
    public void close() throws IOException {

    }
}
