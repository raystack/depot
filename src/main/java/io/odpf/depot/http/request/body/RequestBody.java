package io.odpf.depot.http.request.body;

import io.odpf.depot.message.MessageContainer;

import java.io.IOException;

public interface RequestBody {

    String build(MessageContainer messageContainer) throws IOException;
}
