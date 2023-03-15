package com.gotocompany.depot.message.json;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.JsonOrgJsonProvider;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.utils.JsonUtils;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.exception.EmptyMessageException;
import com.gotocompany.depot.message.MessageUtils;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.JsonParserMetrics;
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
    public ParsedMessage parse(Message message, SinkConnectorSchemaMessageMode type, String schemaClass) throws IOException {
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

}
