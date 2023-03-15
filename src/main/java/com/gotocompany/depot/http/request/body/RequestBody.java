package com.gotocompany.depot.http.request.body;

import com.gotocompany.depot.message.MessageContainer;

import java.io.IOException;

public interface RequestBody {

    String build(MessageContainer messageContainer) throws IOException;
}
