package io.odpf.depot.http.request.builder;

import io.odpf.depot.TestBookingLogKey;
import io.odpf.depot.TestBookingLogMessage;
import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.exception.InvalidTemplateException;
import io.odpf.depot.message.MessageContainer;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParserFactory;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.message.SchemaContainer;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import io.odpf.depot.message.proto.ProtoOdpfMessageParser;
import io.odpf.depot.metrics.StatsDReporter;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class UriBuilderTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private HttpSinkConfig sinkConfig;
    @Mock
    private StatsDReporter statsDReporter;
    @Mock
    private MessageContainer messageContainer;
    @Mock
    private SchemaContainer schemaContainer;
    private final Map<String, String> queryParam = new HashMap<>();
    private final Map<String, String> configuration = new HashMap<>();

    @Before
    public void setUp() throws IOException {
        MockitoAnnotations.openMocks(this);
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "io.odpf.depot.TestBookingLogMessage");
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS", "io.odpf.depot.TestBookingLogKey");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        TestBookingLogKey bookingLogKey = TestBookingLogKey.newBuilder().setOrderNumber("test-order").setOrderUrl("test-url").build();
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setOrderUrl("test-url").build();
        OdpfMessage message = new OdpfMessage(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray());
        ProtoOdpfMessageParser parser = (ProtoOdpfMessageParser) OdpfMessageParserFactory.getParser(sinkConfig, statsDReporter);
        ParsedOdpfMessage parsedMessage = parser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, sinkConfig.getSinkConnectorSchemaProtoMessageClass());
        when(messageContainer.getParsedLogMessage(sinkConfig.getSinkConnectorSchemaProtoMessageClass())).thenReturn(parsedMessage);
    }

    @Test
    public void shouldReturnURIInstanceBasedOnBaseUrl() throws URISyntaxException, InvalidTemplateException {
        configuration.put("SINK_HTTP_SERVICE_URL", "http://dummy.com   ");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        UriBuilder uriBuilder = new UriBuilder(sinkConfig);
        assertEquals(new URI("http://dummy.com"), uriBuilder.build(queryParam));
    }

    @Test
    public void shouldFailWhenUrlConfigIsEmpty() throws InvalidTemplateException {
        expectedException.expect(InvalidTemplateException.class);
        expectedException.expectMessage("Template cannot be empty");
        configuration.put("SINK_HTTP_SERVICE_URL", "");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        UriBuilder uriBuilder = new UriBuilder(sinkConfig);
        uriBuilder.build(queryParam);
    }

    @Test
    public void shouldFailWhenUrlConfigIsNotGiven() throws InvalidTemplateException {
        expectedException.expect(InvalidTemplateException.class);
        expectedException.expectMessage("Template cannot be empty");
        UriBuilder uriBuilder = new UriBuilder(sinkConfig);
        uriBuilder.build(queryParam);
    }

    @Test
    public void shouldFailWhenUrlConfigIsInvalid() throws InvalidTemplateException {
        expectedException.expect(ConfigurationException.class);
        expectedException.expectMessage("Service URL 'http://dummy.com?s=^IXIC' is invalid");
        configuration.put("SINK_HTTP_SERVICE_URL", "http://dummy.com?s=^IXIC");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        UriBuilder uriBuilder = new UriBuilder(sinkConfig);
        uriBuilder.build(queryParam);
    }

    @Test
    public void shouldAddParameter() throws URISyntaxException, InvalidTemplateException {
        queryParam.put("test-key", "test-value");
        configuration.put("SINK_HTTP_SERVICE_URL", "http://dummy.com");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        UriBuilder uriBuilder = new UriBuilder(sinkConfig);
        assertEquals(new URI("http://dummy.com?test-key=test-value"), uriBuilder.build(queryParam));
    }

    @Test
    public void shouldAddMultipleParameter() throws URISyntaxException, InvalidTemplateException {
        queryParam.put("test-key", "test-value");
        configuration.put("SINK_HTTP_SERVICE_URL", "http://dummy.com");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        UriBuilder uriBuilder = new UriBuilder(sinkConfig);
        assertEquals(new URI("http://dummy.com?test-key=test-value"), uriBuilder.build(queryParam));
    }

    @Test
    public void shouldParseUriTemplate() throws URISyntaxException, InvalidTemplateException, IOException {
        configuration.put("SINK_HTTP_SERVICE_URL", "http://dummy.com/%s,order_url");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        UriBuilder uriBuilder = new UriBuilder(sinkConfig);
        assertEquals(new URI("http://dummy.com/test-url"), uriBuilder.build(messageContainer, schemaContainer, queryParam));
    }

    @Test
    public void shouldReturnParsedUriTemplateWithQueryParam() throws URISyntaxException, InvalidTemplateException, IOException {
        queryParam.put("test-key-1", "test-value-1");
        queryParam.put("test-key-2", "test-value-2");
        configuration.put("SINK_HTTP_SERVICE_URL", "http://dummy.com/%s,order_url");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);
        UriBuilder uriBuilder = new UriBuilder(sinkConfig);
        assertEquals(new URI("http://dummy.com/test-url?test-key-1=test-value-1&test-key-2=test-value-2"), uriBuilder.build(messageContainer, schemaContainer, queryParam));
    }
}
