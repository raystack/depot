package io.odpf.depot.redis.parsers;


import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.redis.dataentry.RedisDataEntry;
import io.odpf.depot.redis.models.RedisRecord;
import io.odpf.depot.redis.models.RedisRecords;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Convert kafka messages to RedisDataEntry.
 */
@AllArgsConstructor
public abstract class RedisParser {
    private OdpfMessageParser odpfMessageParser;

    private RedisSinkConfig redisSinkConfig;

    public abstract List<RedisDataEntry> parse(OdpfMessage message);

    public RedisRecords convert(List<OdpfMessage> messages) {
        List<RedisRecord> valid = new ArrayList<>();
        List<RedisRecord> invalid = new ArrayList<>();
        for(int i = 0; i < messages.size(); i++) {
            try {
                List<RedisDataEntry> p = parse(messages.get(i));
                for(int ii = 0; ii < p.size(); ii++) {
                    valid.add(new RedisRecord(p.get(ii), (long) valid.size(), new ErrorInfo(null ,null)));
                }
            }
            catch (Exception e) {
                // generic handler
                invalid.add(new RedisRecord(null, (long) i, new ErrorInfo(e, ErrorType.DEFAULT_ERROR)));
            }
        }
        return new RedisRecords(valid, invalid);
    }
}
