package com.gotocompany.depot.http.request.body;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.MessageUtils;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.schema.LogicalType;
import com.gotocompany.depot.schema.SchemaFieldType;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;
import java.util.stream.Collectors;

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
        Map<String, Object> timestampProperties = getTimeStampProperties(parsedMessage);
        timestampProperties.forEach(jsonMessage::put);
        return jsonMessage.toString();
    }

    private Map<String, Object> getTimeStampProperties(ParsedMessage parsedMessage) {
        return parsedMessage.getFields().entrySet().stream()
                .filter(sf -> {
                    if (sf.getKey().getType().equals(SchemaFieldType.MESSAGE) && !(sf.getKey().isRepeated())) {
                        return ((ParsedMessage) sf.getValue()).getSchema().logicalType().equals(LogicalType.TIMESTAMP)
                                && config.isSinkHttpDateFormatEnabled().equals(true);
                    }
                    return false;
                })
                .collect(Collectors.toMap(
                        sf -> sf.getKey().getJsonName(),
                        v -> getTimestampValue(v.getValue())
                ));
    }

    private Object getTimestampValue(Object value) {
        Instant time = ((ParsedMessage) value).getLogicalValue().getTimestamp();
        LocalDateTime datetime = LocalDateTime.ofInstant(time, ZoneOffset.UTC);
        DateTimeFormatter format = DateTimeFormatter.ofPattern("MMM d, yyyy h:mm:ss a");
        return datetime.format(format);
    }

    private String removePrefixMetadata(String metadataFieldName) {
        return StringUtils.removeStart(metadataFieldName, METADATA_PREFIX);
    }
}
