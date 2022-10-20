package io.odpf.depot.http.request;

import io.odpf.depot.http.record.HttpRequestRecord;
import io.odpf.depot.message.OdpfMessage;

import java.util.List;

public class BatchRequest implements Request {

    @Override
    public List<HttpRequestRecord> createRecords(List<OdpfMessage> messages) {
        return null;
    }
}
