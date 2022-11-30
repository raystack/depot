package io.odpf.depot.message.json;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.JsonOrgJsonProvider;
import io.odpf.depot.config.OdpfSinkConfig;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.exception.EmptyMessageException;
import io.odpf.depot.message.MessageUtils;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.JsonParserMetrics;
import io.odpf.depot.utils.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.time.Instant;

@Slf4j
public class JsonOdpfMessageParser implements OdpfMessageParser {

    private final OdpfSinkConfig config;
    private final Instrumentation instrumentation;
    private final JsonParserMetrics jsonParserMetrics;
    private final Configuration jsonPathConfig = Configuration.builder()
            .jsonProvider(new JsonOrgJsonProvider())
            .build();

    public JsonOdpfMessageParser(OdpfSinkConfig config, Instrumentation instrumentation, JsonParserMetrics jsonParserMetrics) {
        this.instrumentation = instrumentation;
        this.jsonParserMetrics = jsonParserMetrics;
        this.config = config;

    }

    @Override
    public ParsedOdpfMessage parse(OdpfMessage message, SinkConnectorSchemaMessageMode type, String schemaClass) throws IOException {
        if (type == null) {
            throw new IOException("message mode not defined");
        }
        MessageUtils.validate(message, byte[].class);
        byte[] payload;
        switch (type) {
            case LOG_KEY:
                payload = (byte[]) message.getLogKey();
                break;
            case LOG_MESSAGE:
                payload = (byte[]) message.getLogMessage();
                break;
            default:
                throw new ConfigurationException("Schema type not supported");
        }
        try {
            if (payload == null || payload.length == 0) {
                log.info("empty message found {}", message.getMetadataString());
                throw new EmptyMessageException();
            }
            Instant instant = Instant.now();
            JSONObject jsonObject = JsonUtils.getJsonObject(config, payload);
            instrumentation.captureDurationSince(jsonParserMetrics.getJsonParseTimeTakenMetric(), instant);
            return new JsonOdpfParsedMessage(jsonObject, jsonPathConfig);
        } catch (JSONException ex) {
            throw new IOException("invalid json error", ex);
        }
    }

    @Override
    public OdpfMessageSchema getSchema(String schemaClass) {
        return null;
    }
}
