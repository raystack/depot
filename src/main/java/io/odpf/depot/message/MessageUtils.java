package io.odpf.depot.message;

import io.odpf.depot.common.TupleString;
import io.odpf.depot.config.OdpfSinkConfig;

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
}
