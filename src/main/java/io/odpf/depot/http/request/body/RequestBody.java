package io.odpf.depot.http.request.body;

import io.odpf.depot.message.OdpfMessage;

public interface RequestBody {

    String build(OdpfMessage odpfMessage);
}
