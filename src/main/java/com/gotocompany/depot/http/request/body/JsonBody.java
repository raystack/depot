package com.gotocompany.depot.http.request.body;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.MessageUtils;
import com.gotocompany.depot.message.ParsedMessage;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Date;

public class JsonBody implements RequestBody {

    private static final String METADATA_PREFIX = "message_";

    private final HttpSinkConfig config;

    public JsonBody(HttpSinkConfig config) {
        this.config = config;
    }

    @Override
    public String build(MessageContainer messageContainer) throws IOException {
        ParsedMessage parsedLogKey = messageContainer.getParsedLogKey(config.getSinkConnectorSchemaProtoKeyClass());
        ParsedMessage parsedLogMessage = messageContainer.getParsedLogMessage(config.getSinkConnectorSchemaProtoMessageClass());
        JSONObject payload = new JSONObject();
        payload.put("logKey", buildJsonMessage(parsedLogKey));
        payload.put("logMessage", buildJsonMessage(parsedLogMessage));
        MessageUtils.getMetaData(messageContainer.getMessage(), config, Date::new)
                .forEach((key, value) -> payload.put(removePrefixMetadata(key), value));
        return payload.toString();
    }

    private String buildJsonMessage(ParsedMessage parsedMessage) {
        JSONObject jsonMessage = parsedMessage.toJson();
        return jsonMessage.toString();
    }

    private String removePrefixMetadata(String metadataFieldName) {
        return StringUtils.removeStart(metadataFieldName, METADATA_PREFIX);
    }
}
