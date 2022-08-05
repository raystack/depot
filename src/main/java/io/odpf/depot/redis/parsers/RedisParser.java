package io.odpf.depot.redis.parsers;

import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.exception.DeserializerException;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.redis.dataentry.RedisDataEntry;
import io.odpf.depot.redis.models.RedisRecord;
import io.odpf.depot.redis.models.RedisRecords;
import lombok.AllArgsConstructor;
import io.odpf.depot.message.ParsedOdpfMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;



/**
 * Convert kafka messages to RedisDataEntry.
 */
@AllArgsConstructor
public abstract class RedisParser {
    private OdpfMessageParser odpfMessageParser;

    private RedisSinkConfig redisSinkConfig;

    public abstract List<RedisDataEntry> parse(OdpfMessage message);

    String parseKeyTemplate(String template, ParsedOdpfMessage parsedOdpfMessage) throws IOException {
        if (template.isEmpty() || template.equals("")) {
            throw new ConfigurationException("Set config SINK_REDIS_KEY_TEMPLATE");
        }
        String key = "";
        String field = "";
        for (int i = 0; i < template.length(); i++) {
            char c = template.charAt(i);
            if (c == '{') {
                for (int ii = i + 1; ii < template.length(); ii++) {
                    char f = template.charAt(ii);
                    if (f == '}') {
                        i = ii;
                        break;
                    }
                    field += f;
                }
                key += parsedOdpfMessage.getFieldByName(field);
                field = "";
            } else {
                key += c;
            }

        }
        return key;
    }

    public RedisRecords convert(List<OdpfMessage> messages) {
        List<RedisRecord> valid = new ArrayList<>();
        List<RedisRecord> invalid = new ArrayList<>();
        for (int i = 0; i < messages.size(); i++) {
            try {
                List<RedisDataEntry> p = parse(messages.get(i));
                for (int ii = 0; ii < p.size(); ii++) {
                    valid.add(new RedisRecord(p.get(ii), (long) valid.size(), new ErrorInfo(null, null)));
                }
            } catch (ConfigurationException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.UNKNOWN_FIELDS_ERROR);
                invalid.add(new RedisRecord(null, (long) i, errorInfo));
            } catch (DeserializerException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR);
                invalid.add(new RedisRecord(null, (long) i, errorInfo));
            }
        }
        return new RedisRecords(valid, invalid);
    }
}
