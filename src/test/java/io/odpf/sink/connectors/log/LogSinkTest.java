package io.odpf.sink.connectors.log;

import io.odpf.sink.connectors.OdpfSinkResponse;
import io.odpf.sink.connectors.config.OdpfSinkConfig;
import io.odpf.sink.connectors.error.ErrorInfo;
import io.odpf.sink.connectors.error.ErrorType;
import io.odpf.sink.connectors.expcetion.OdpfSinkException;
import io.odpf.sink.connectors.message.json.JsonOdpfMessage;
import io.odpf.sink.connectors.message.json.JsonOdpfMessageParser;
import io.odpf.sink.connectors.message.OdpfMessage;
import io.odpf.sink.connectors.message.OdpfMessageParser;
import io.odpf.sink.connectors.metrics.Instrumentation;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class LogSinkTest {
    private final String template = "\n================= DATA =======================\n{}\n================= METADATA =======================\n{}\n";
    private OdpfSinkConfig config;
    private OdpfMessageParser messageParser;
    private Instrumentation instrumentation;

    @Before
    public void setUp() throws Exception {
         config = mock(OdpfSinkConfig.class);
         messageParser = mock(OdpfMessageParser.class);
         instrumentation = mock(Instrumentation.class);

    }

    @Test
    public void shouldProcessEmptyMessageWithNoError() throws IOException {
        LogSink logSink = new LogSink(config, messageParser, instrumentation);
        ArrayList<OdpfMessage> messages = new ArrayList<>();
        OdpfSinkResponse odpfSinkResponse = logSink.pushToSink(messages);
        Map<Long, List<ErrorInfo>> errors = odpfSinkResponse.getErrors();

        assertEquals(Collections.emptyMap(), errors);
        verify(messageParser, never()).parse(any(), any(), any());
        verify(instrumentation, never()).logInfo(any(), any(), any());
    }

    @Test
    public void shouldLogJsonMessages() throws OdpfSinkException {
        HashMap<String, String> configMap = new HashMap<String, String>() {{
            put("INPUT_SCHEMA_MESSAGE_MODE", "log_message");
        }};
        OdpfSinkConfig odpfSinkConfig = ConfigFactory.create(OdpfSinkConfig.class, configMap);
        OdpfMessageParser messageParser = new JsonOdpfMessageParser(odpfSinkConfig);
        LogSink logSink = new LogSink(odpfSinkConfig, messageParser, instrumentation);
        ArrayList<OdpfMessage> messages = new ArrayList<>();
        String validJsonFirstName = "{\"first_name\":\"john\"}";
        byte[] logMessage1 = validJsonFirstName.getBytes();
        String validJsonLastName = "{\"last_name\":\"doe\"}";
        byte[] logMessage2 = validJsonLastName.getBytes();
        messages.add(new JsonOdpfMessage(null, logMessage1));
        messages.add(new JsonOdpfMessage(null, logMessage2));
        OdpfSinkResponse odpfSinkResponse = logSink.pushToSink(messages);

        //assert no error
        Map<Long, List<ErrorInfo>> errors = odpfSinkResponse.getErrors();
        assertEquals(Collections.emptyMap(), errors);

        //assert processed message
        ArgumentCaptor<String> jsonStrCaptor = ArgumentCaptor.forClass(String.class);
        verify(instrumentation, times(2)).logInfo(eq(template), jsonStrCaptor.capture(), eq(Collections.emptyMap().toString()));
        assertThat(jsonStrCaptor.getAllValues(), containsInAnyOrder(validJsonFirstName, validJsonLastName));
    }

    @Test
    public void shouldReturnErrorResponseAndProcessValidMessage() throws OdpfSinkException {
        HashMap<String, String> configMap = new HashMap<String, String>() {{
            put("INPUT_SCHEMA_MESSAGE_MODE", "log_message");
        }};
        OdpfSinkConfig odpfSinkConfig = ConfigFactory.create(OdpfSinkConfig.class, configMap);

        OdpfMessageParser messageParser = new JsonOdpfMessageParser(odpfSinkConfig);
        LogSink logSink = new LogSink(odpfSinkConfig, messageParser, instrumentation);
        ArrayList<OdpfMessage> messages = new ArrayList<>();
        String validJsonFirstName = "{\"first_name\":\"john\"}";
        byte[] logMessage1 = validJsonFirstName.getBytes();
        String invalidJson = "{\"last_name";
        byte[] invalidLogMessage = invalidJson.getBytes();
        messages.add(new JsonOdpfMessage(null, logMessage1));
        messages.add(new JsonOdpfMessage(null, invalidLogMessage));
        OdpfSinkResponse odpfSinkResponse = logSink.pushToSink(messages);

        //assert error
        Map<Long, List<ErrorInfo>> errors = odpfSinkResponse.getErrors();
        assertEquals(1, errors.size());
        List<ErrorInfo> errorInfos = errors.get(1L);
        assertEquals(1, errorInfos.size());
        assertEquals(ErrorType.DESERIALIZATION_ERROR, errorInfos.get(0).getErrorType());

        //assert valid message processed
        ArgumentCaptor<String> jsonStrCaptor = ArgumentCaptor.forClass(String.class);
        verify(instrumentation, times(1)).logInfo(eq(template), jsonStrCaptor.capture(), eq(Collections.emptyMap().toString()));
        assertEquals(validJsonFirstName, jsonStrCaptor.getValue().toString());
    }
}
