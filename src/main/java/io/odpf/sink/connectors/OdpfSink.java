package io.odpf.sink.connectors;

import io.odpf.sink.connectors.expcetion.OdpfSinkException;
import io.odpf.sink.connectors.message.OdpfMessage;
import lombok.Getter;

import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface OdpfSink extends Closeable {

    OdpfSinkResponse pushToSink(List<OdpfMessage> messages) throws OdpfSinkException;
}


