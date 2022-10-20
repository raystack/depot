package io.odpf.depot.http.request;

import io.odpf.depot.http.record.HttpRequestRecord;
import io.odpf.depot.message.OdpfMessage;

import java.util.List;

public interface Request {

    List<HttpRequestRecord> createRecords(List<OdpfMessage> messages);
}
