package io.odpf.sink.connectors.log;

import io.odpf.sink.connectors.OdpfSink;
import io.odpf.sink.connectors.OdpfSinkResponse;
import io.odpf.sink.connectors.config.OdpfSinkConfig;
import io.odpf.sink.connectors.error.ErrorInfo;
import io.odpf.sink.connectors.error.ErrorType;
import io.odpf.sink.connectors.expcetion.OdpfSinkException;
import io.odpf.sink.connectors.message.OdpfMessage;
import io.odpf.sink.connectors.message.OdpfMessageParser;
import io.odpf.sink.connectors.message.ParsedOdpfMessage;
import io.odpf.sink.connectors.metrics.Instrumentation;

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
        for (int ii = 0; ii < messages.size(); ii++) {
            OdpfMessage message = messages.get(ii);
            try {
                ParsedOdpfMessage parsedOdpfMessage =
                        odpfMessageParser.parse(
                                message,
                                config.getInputSchemaMessageMode(),
                                config.getInputSchemaProtoClass());
                instrumentation.logInfo("\n================= DATA =======================\n{}" +
                                "\n================= METADATA =======================\n{}\n",
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
