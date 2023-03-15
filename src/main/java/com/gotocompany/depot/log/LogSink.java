package com.gotocompany.depot.log;

import com.gotocompany.depot.Sink;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.SinkException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.SinkResponse;

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
                ? config.getSinkConnectorSchemaProtoMessageClass() : config.getSinkConnectorSchemaProtoKeyClass();
        for (int ii = 0; ii < messages.size(); ii++) {
            Message message = messages.get(ii);
            try {
                ParsedMessage parsedMessage =
                        messageParser.parse(
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
