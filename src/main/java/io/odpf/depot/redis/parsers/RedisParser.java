package io.odpf.depot.redis.parsers;

import io.odpf.depot.common.Tuple;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.exception.DeserializerException;
import io.odpf.depot.message.*;
import io.odpf.depot.redis.client.entry.RedisEntry;
import io.odpf.depot.redis.record.RedisRecord;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;


/**
 * Convert Odpf messages to RedisRecords.
 */

@AllArgsConstructor
public class RedisParser {
    private final OdpfMessageParser odpfMessageParser;
    private final RedisEntryParser redisEntryParser;
    private final OdpfMessageSchema schema;
    private final Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema;

    public List<RedisRecord> convert(List<OdpfMessage> messages) {
        List<RedisRecord> records = new ArrayList<>();
        IntStream.range(0, messages.size()).forEach(index -> {
            try {
                ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(messages.get(index), modeAndSchema.getFirst(), modeAndSchema.getSecond());
                List<RedisEntry> redisDataEntries = redisEntryParser.getRedisEntry(parsedOdpfMessage, schema);
                for (RedisEntry redisEntry : redisDataEntries) {
                    records.add(new RedisRecord(redisEntry, (long) index, null, messages.get(index).getMetadataString(), true));
                }
            } catch (ConfigurationException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.UNKNOWN_FIELDS_ERROR);
                records.add(new RedisRecord(null, (long) index, errorInfo, messages.get(index).getMetadataString(), false));
            } catch (IllegalArgumentException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.DEFAULT_ERROR);
                records.add(new RedisRecord(null, (long) index, errorInfo, messages.get(index).getMetadataString(), false));
            } catch (DeserializerException | IOException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR);
                records.add(new RedisRecord(null, (long) index, errorInfo, messages.get(index).getMetadataString(), false));
            }
        });
        return records;
    }
}
