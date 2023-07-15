package org.raystack.depot.log;

import org.raystack.depot.message.json.JsonRaystackMessageParser;
import org.raystack.depot.RaystackSinkResponse;
import org.raystack.depot.config.RaystackSinkConfig;
import org.raystack.depot.error.ErrorInfo;
import org.raystack.depot.error.ErrorType;
import org.raystack.depot.exception.RaystackSinkException;
import org.raystack.depot.message.RaystackMessage;
import org.raystack.depot.message.RaystackMessageParser;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.metrics.JsonParserMetrics;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
    private RaystackSinkConfig config;
    private RaystackMessageParser raystackMessageParser;
    private Instrumentation instrumentation;
    private JsonParserMetrics jsonParserMetrics;

    @Before
    public void setUp() throws Exception {
        config = mock(RaystackSinkConfig.class);
        raystackMessageParser = mock(RaystackMessageParser.class);
        instrumentation = mock(Instrumentation.class);
        jsonParserMetrics = new JsonParserMetrics(config);

    }

    @Test
    public void shouldProcessEmptyMessageWithNoError() throws IOException {
        LogSink logSink = new LogSink(config, raystackMessageParser, instrumentation);
        ArrayList<RaystackMessage> messages = new ArrayList<>();
        RaystackSinkResponse raystackSinkResponse = logSink.pushToSink(messages);
        Map<Long, ErrorInfo> errors = raystackSinkResponse.getErrors();

        assertEquals(Collections.emptyMap(), errors);
        verify(raystackMessageParser, never()).parse(any(), any(), any());
        verify(instrumentation, never()).logInfo(any(), any(), any());
    }

    @Test
    public void shouldLogJsonMessages() throws RaystackSinkException {
        HashMap<String, String> configMap = new HashMap<String, String>() {
            {
                put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", "log_message");
            }
        };
        RaystackSinkConfig raystackSinkConfig = ConfigFactory.create(RaystackSinkConfig.class, configMap);
        RaystackMessageParser messageParser = new JsonRaystackMessageParser(raystackSinkConfig, instrumentation,
                jsonParserMetrics);
        LogSink logSink = new LogSink(raystackSinkConfig, messageParser, instrumentation);
        ArrayList<RaystackMessage> messages = new ArrayList<>();
        String validJsonFirstName = "{\"first_name\":\"john\"}";
        byte[] logMessage1 = validJsonFirstName.getBytes();
        String validJsonLastName = "{\"last_name\":\"doe\"}";
        byte[] logMessage2 = validJsonLastName.getBytes();
        messages.add(new RaystackMessage(null, logMessage1));
        messages.add(new RaystackMessage(null, logMessage2));
        RaystackSinkResponse raystackSinkResponse = logSink.pushToSink(messages);

        // assert no error
        Map<Long, ErrorInfo> errors = raystackSinkResponse.getErrors();
        assertEquals(Collections.emptyMap(), errors);

        // assert processed message
        ArgumentCaptor<String> jsonStrCaptor = ArgumentCaptor.forClass(String.class);
        verify(instrumentation, times(2)).logInfo(eq(template), jsonStrCaptor.capture(),
                eq(Collections.emptyMap().toString()));
        assertThat(jsonStrCaptor.getAllValues(), containsInAnyOrder(validJsonFirstName, validJsonLastName));
    }

    @Test
    public void shouldReturnErrorResponseAndProcessValidMessage() throws RaystackSinkException {
        HashMap<String, String> configMap = new HashMap<String, String>() {
            {
                put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", "log_message");
            }
        };
        RaystackSinkConfig raystackSinkConfig = ConfigFactory.create(RaystackSinkConfig.class, configMap);

        RaystackMessageParser messageParser = new JsonRaystackMessageParser(raystackSinkConfig, instrumentation,
                jsonParserMetrics);
        LogSink logSink = new LogSink(raystackSinkConfig, messageParser, instrumentation);
        ArrayList<RaystackMessage> messages = new ArrayList<>();
        String validJsonFirstName = "{\"first_name\":\"john\"}";
        byte[] logMessage1 = validJsonFirstName.getBytes();
        String invalidJson = "{\"last_name";
        byte[] invalidLogMessage = invalidJson.getBytes();
        messages.add(new RaystackMessage(null, logMessage1));
        messages.add(new RaystackMessage(null, invalidLogMessage));
        RaystackSinkResponse raystackSinkResponse = logSink.pushToSink(messages);

        // assert error
        ErrorInfo error = raystackSinkResponse.getErrorsFor(1L);
        assertEquals(ErrorType.DESERIALIZATION_ERROR, error.getErrorType());

        // assert valid message processed
        ArgumentCaptor<String> jsonStrCaptor = ArgumentCaptor.forClass(String.class);
        verify(instrumentation, times(1)).logInfo(eq(template), jsonStrCaptor.capture(),
                eq(Collections.emptyMap().toString()));
        assertEquals(validJsonFirstName, jsonStrCaptor.getValue().toString());
    }
}
