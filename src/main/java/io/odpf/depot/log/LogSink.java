package org.raystack.depot.log;

import org.raystack.depot.OdpfSink;
import org.raystack.depot.OdpfSinkResponse;
import org.raystack.depot.config.OdpfSinkConfig;
import org.raystack.depot.error.ErrorInfo;
import org.raystack.depot.error.ErrorType;
import org.raystack.depot.exception.OdpfSinkException;
import org.raystack.depot.message.SinkConnectorSchemaMessageMode;
import org.raystack.depot.message.OdpfMessage;
import org.raystack.depot.message.OdpfMessageParser;
import org.raystack.depot.message.ParsedOdpfMessage;
import org.raystack.depot.metrics.Instrumentation;

import java.io.IOException;
import java.util.List;

public class LogSink implements OdpfSink {
    private final OdpfMessageParser raystackMessageParser;
    private final Instrumentation instrumentation;
    private final OdpfSinkConfig config;

    public LogSink(OdpfSinkConfig config, OdpfMessageParser raystackMessageParser, Instrumentation instrumentation) {
        this.raystackMessageParser = raystackMessageParser;
        this.instrumentation = instrumentation;
        this.config = config;
    }

    @Override
    public OdpfSinkResponse pushToSink(List<OdpfMessage> messages) throws OdpfSinkException {
        OdpfSinkResponse response = new OdpfSinkResponse();
        SinkConnectorSchemaMessageMode mode = config.getSinkConnectorSchemaMessageMode();
        String schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? config.getSinkConnectorSchemaProtoMessageClass()
                : config.getSinkConnectorSchemaProtoKeyClass();
        for (int ii = 0; ii < messages.size(); ii++) {
            OdpfMessage message = messages.get(ii);
            try {
                ParsedOdpfMessage parsedOdpfMessage = raystackMessageParser.parse(
                        message,
                        mode,
                        schemaClass);
                instrumentation.logInfo("\n================= DATA =======================\n{}"
                        + "\n================= METADATA =======================\n{}\n",
                        parsedOdpfMessage.toString(), message.getMetadataString());
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
