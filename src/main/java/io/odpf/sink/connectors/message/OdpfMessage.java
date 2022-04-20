package io.odpf.sink.connectors.message;

import java.util.Map;

public interface OdpfMessage {

    String getMetadataString();

    Map<String, Object> getMetadata();

    byte[] getLogMessage();

    byte[] getLogKey();


}
