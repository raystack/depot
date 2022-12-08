package io.odpf.depot.http.request;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.http.record.HttpRequestRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

import java.util.Map;

@Slf4j
public class RequestUtils {

    protected static HttpRequestRecord createErrorRecord(Exception e, ErrorType type, Integer index, Map<String, Object> metadata) {
        ErrorInfo errorInfo = new ErrorInfo(e, type);
        log.error("Error while parsing record for message. Metadata : {}, Error: {}", metadata, errorInfo);
        HttpRequestRecord record = new HttpRequestRecord(errorInfo);
        record.addIndex(index);
        return record;
    }

    public static StringEntity buildStringEntity(Object input) {
        return new StringEntity(input.toString(), ContentType.APPLICATION_JSON);
    }
}
