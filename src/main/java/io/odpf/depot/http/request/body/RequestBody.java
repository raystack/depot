package io.odpf.depot.http.request.body;

import io.odpf.depot.message.OdpfMessage;

import java.io.IOException;

public interface RequestBody {

    String build(OdpfMessage odpfMessage) throws IOException;
}
