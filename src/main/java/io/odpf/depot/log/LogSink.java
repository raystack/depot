package io.odpf.depot.log;

import io.odpf.depot.OdpfSink;
import io.odpf.depot.OdpfSinkResponse;
import io.odpf.depot.config.OdpfSinkConfig;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.expcetion.OdpfSinkException;
import io.odpf.depot.message.InputSchemaMessageMode;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.metrics.Instrumentation;

import java.io.IOException;
import java.util.List;

public class LogSink implements OdpfSink {
    private final OdpfMessageParser odpfMessageParser;
    private final Instrumentation instrumentation;
    private final OdpfSinkConfig config;

    public LogSink(OdpfSinkConfig config, OdpfMessageParser odpfMessageParser, Instrumentation instrumentation) {
        this.odpfMessageParser = odpfMessageParser;
        this.instrumentation = instrumentation;
        this.config = config;
    }

    @Override
    public OdpfSinkResponse pushToSink(List<OdpfMessage> messages) throws OdpfSinkException {
        OdpfSinkResponse response = new OdpfSinkResponse();
        InputSchemaMessageMode mode = config.getSinkConnectorSchemaMessageMode();
        String schemaClass = mode == InputSchemaMessageMode.LOG_MESSAGE
                ? config.getSinkConnectorSchemaMessageClass() : config.getSinkConnectorSchemaKeyClass();
        for (int ii = 0; ii < messages.size(); ii++) {
            OdpfMessage message = messages.get(ii);
            try {
                ParsedOdpfMessage parsedOdpfMessage =
                        odpfMessageParser.parse(
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
