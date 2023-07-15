package org.raystack.depot.message.json;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.JsonOrgJsonProvider;
import org.raystack.depot.config.SinkConfig;
import org.raystack.depot.utils.JsonUtils;
import org.raystack.depot.exception.ConfigurationException;
import org.raystack.depot.exception.EmptyMessageException;
import org.raystack.depot.message.MessageUtils;
import org.raystack.depot.message.Message;
import org.raystack.depot.message.MessageParser;
import org.raystack.depot.message.MessageSchema;
import org.raystack.depot.message.ParsedMessage;
import org.raystack.depot.message.SinkConnectorSchemaMessageMode;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.metrics.JsonParserMetrics;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.time.Instant;

@Slf4j
public class JsonMessageParser implements MessageParser {

    private final SinkConfig config;
    private final Instrumentation instrumentation;
    private final JsonParserMetrics jsonParserMetrics;
    private final Configuration jsonPathConfig = Configuration.builder()
            .jsonProvider(new JsonOrgJsonProvider())
            .build();

    public JsonMessageParser(SinkConfig config, Instrumentation instrumentation, JsonParserMetrics jsonParserMetrics) {
        this.instrumentation = instrumentation;
        this.jsonParserMetrics = jsonParserMetrics;
        this.config = config;

    }

    @Override
    public ParsedMessage parse(Message message, SinkConnectorSchemaMessageMode type, String schemaClass)
            throws IOException {
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
            return new JsonParsedMessage(jsonObject, jsonPathConfig);
        } catch (JSONException ex) {
            throw new IOException("invalid json error", ex);
        }
    }

    @Override
    public MessageSchema getSchema(String schemaClass) {
        return null;
    }
}
