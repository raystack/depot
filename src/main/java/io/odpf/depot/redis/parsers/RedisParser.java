package org.raystack.depot.redis.parsers;

import org.raystack.depot.common.Tuple;
import org.raystack.depot.error.ErrorInfo;
import org.raystack.depot.error.ErrorType;
import org.raystack.depot.exception.ConfigurationException;
import org.raystack.depot.exception.DeserializerException;
import org.raystack.depot.message.OdpfMessage;
import org.raystack.depot.message.OdpfMessageParser;
import org.raystack.depot.message.ParsedOdpfMessage;
import org.raystack.depot.message.SinkConnectorSchemaMessageMode;
import org.raystack.depot.redis.client.entry.RedisEntry;
import org.raystack.depot.redis.record.RedisRecord;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Convert Odpf messages to RedisRecords.
 */

@AllArgsConstructor
@Slf4j
public class RedisParser {
    private final OdpfMessageParser raystackMessageParser;
    private final RedisEntryParser redisEntryParser;
    private final Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema;

    public List<RedisRecord> convert(List<OdpfMessage> messages) {
        List<RedisRecord> records = new ArrayList<>();
        IntStream.range(0, messages.size()).forEach(index -> {
            try {
                ParsedOdpfMessage parsedOdpfMessage = raystackMessageParser.parse(messages.get(index),
                        modeAndSchema.getFirst(), modeAndSchema.getSecond());
                List<RedisEntry> redisDataEntries = redisEntryParser.getRedisEntry(parsedOdpfMessage);
                for (RedisEntry redisEntry : redisDataEntries) {
                    records.add(new RedisRecord(redisEntry, (long) index, null, messages.get(index).getMetadataString(),
                            true));
                }
            } catch (UnsupportedOperationException e) {
                records.add(createAndLogErrorRecord(e, ErrorType.INVALID_MESSAGE_ERROR, index, messages));
            } catch (ConfigurationException e) {
                records.add(createAndLogErrorRecord(e, ErrorType.UNKNOWN_FIELDS_ERROR, index, messages));
            } catch (IllegalArgumentException e) {
                records.add(createAndLogErrorRecord(e, ErrorType.DEFAULT_ERROR, index, messages));
            } catch (DeserializerException | IOException e) {
                records.add(createAndLogErrorRecord(e, ErrorType.DESERIALIZATION_ERROR, index, messages));
            }
        });
        return records;
    }

    private RedisRecord createAndLogErrorRecord(Exception e, ErrorType type, int index, List<OdpfMessage> messages) {
        ErrorInfo errorInfo = new ErrorInfo(e, type);
        RedisRecord record = new RedisRecord(null, (long) index, errorInfo, messages.get(index).getMetadataString(),
                false);
        log.error("Error while parsing record for message. Record: {}, Error: {}", record, errorInfo);
        return record;
    }
}
