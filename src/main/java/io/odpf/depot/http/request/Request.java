package io.odpf.depot.http.request;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.http.record.HttpRequestRecord;
import io.odpf.depot.message.OdpfMessage;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

import java.util.List;

public interface Request {

    List<HttpRequestRecord> createRecords(List<OdpfMessage> messages);

    default StringEntity buildEntity(String stringBody) {
        return new StringEntity(stringBody, ContentType.APPLICATION_JSON);
    }

    default HttpRequestRecord createErrorRecord(ErrorInfo errorInfo, int index) {
        return new HttpRequestRecord((long) index, errorInfo, false, null);
    }
}
