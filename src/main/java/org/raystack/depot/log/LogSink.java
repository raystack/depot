package org.raystack.depot.log;

import org.raystack.depot.Sink;
import org.raystack.depot.config.SinkConfig;
import org.raystack.depot.error.ErrorInfo;
import org.raystack.depot.error.ErrorType;
import org.raystack.depot.exception.SinkException;
import org.raystack.depot.message.Message;
import org.raystack.depot.message.MessageParser;
import org.raystack.depot.message.ParsedMessage;
import org.raystack.depot.message.SinkConnectorSchemaMessageMode;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.SinkResponse;

import java.io.IOException;
import java.util.List;

public class LogSink implements Sink {
    private final MessageParser messageParser;
    private final Instrumentation instrumentation;
    private final SinkConfig config;

    public LogSink(SinkConfig config, MessageParser messageParser, Instrumentation instrumentation) {
        this.messageParser = messageParser;
        this.instrumentation = instrumentation;
        this.config = config;
    }

    @Override
    public SinkResponse pushToSink(List<Message> messages) throws SinkException {
        SinkResponse response = new SinkResponse();
        SinkConnectorSchemaMessageMode mode = config.getSinkConnectorSchemaMessageMode();
        String schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? config.getSinkConnectorSchemaProtoMessageClass()
                : config.getSinkConnectorSchemaProtoKeyClass();
        for (int ii = 0; ii < messages.size(); ii++) {
            Message message = messages.get(ii);
            try {
                ParsedMessage parsedMessage = messageParser.parse(
                        message,
                        mode,
                        schemaClass);
                instrumentation.logInfo("\n================= DATA =======================\n{}"
                        + "\n================= METADATA =======================\n{}\n",
                        parsedMessage.toString(), message.getMetadataString());
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
