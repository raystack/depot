package com.gotocompany.depot.http.request;

import com.gotocompany.depot.http.record.HttpRequestRecord;
import com.gotocompany.depot.message.Message;

import java.util.List;

public interface Request {
    List<HttpRequestRecord> createRecords(List<Message> messages);

}
