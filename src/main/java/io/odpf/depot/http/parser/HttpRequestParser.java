package io.odpf.depot.http.parser;

import io.odpf.depot.common.Tuple;
import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.http.record.HttpRequestRecord;
import io.odpf.depot.http.request.Request;
import io.odpf.depot.http.request.RequestFactory;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import lombok.AllArgsConstructor;

import java.util.List;

@AllArgsConstructor
public class HttpRequestParser {

    private final OdpfMessageParser messageParser;
    private final Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema;
    private final HttpSinkConfig sinkConfig;

    public List<HttpRequestRecord> convert(List<OdpfMessage> messages) {
        Request request = RequestFactory.create(sinkConfig);

        return request.createRecords(messages);
    }
}
