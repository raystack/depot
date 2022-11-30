package io.odpf.depot.message;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import io.odpf.depot.common.TupleString;
import io.odpf.depot.config.OdpfSinkConfig;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MessageUtils {

    public static Map<String, Object> getMetaData(OdpfMessage message, OdpfSinkConfig config, Function<Long, Object> timeStampConvertor) {
        if (config.shouldAddMetadata()) {
            List<TupleString> metadataColumnsTypes = config.getMetadataColumnsTypes();
            Map<String, Object> metadata = message.getMetadata(metadataColumnsTypes);
            return checkAndSetTimeStampColumns(metadata, metadataColumnsTypes, timeStampConvertor);
        } else {
            return Collections.emptyMap();
        }
    }

    public static Map<String, Object> checkAndSetTimeStampColumns(Map<String, Object> metadata, List<TupleString> metadataColumnsTypes, Function<Long, Object> timeStampConvertor) {
        return metadataColumnsTypes.stream().collect(Collectors.toMap(TupleString::getFirst, t -> {
            String key = t.getFirst();
            String dataType = t.getSecond();
            Object value = metadata.get(key);
            if (value instanceof Long && dataType.equals("timestamp")) {
                return timeStampConvertor.apply((Long) value);
            }
            return value;
        }));
    }

    public static Object getFieldFromJsonObject(String name, JSONObject jsonObject, Configuration jsonPathConfig) {
        try {
            String jsonPathName = "$." + name;
            JsonPath jsonPath = JsonPath.compile(jsonPathName);
            return jsonPath.read(jsonObject, jsonPathConfig);
        } catch (PathNotFoundException e) {
            throw new IllegalArgumentException("Invalid field config : " + name, e);
        }
    }

    public static void validate(OdpfMessage message, Class validClass) throws IOException {
        if ((message.getLogKey() != null && !(validClass.isInstance(message.getLogKey())))
                || (message.getLogMessage() != null && !(validClass.isInstance(message.getLogMessage())))) {
            throw new IOException(
                    String.format("Expected class %s, but found: LogKey class: %s, LogMessage class: %s",
                            validClass,
                            message.getLogKey() != null ? message.getLogKey().getClass() : "n/a",
                            message.getLogMessage() != null ? message.getLogMessage().getClass() : "n/a"));
        }
    }
}
