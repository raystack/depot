package io.odpf.depot.bigtable.parser;

import io.odpf.depot.TestBookingLogKey;
import io.odpf.depot.TestBookingLogMessage;
import io.odpf.depot.TestServiceType;
import io.odpf.depot.bigtable.model.BigTableRecord;
import io.odpf.depot.bigtable.model.BigTableSchema;
import io.odpf.depot.common.Template;
import io.odpf.depot.common.Tuple;
import io.odpf.depot.config.BigTableSinkConfig;
import io.odpf.depot.exception.InvalidTemplateException;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import io.odpf.depot.message.proto.ProtoOdpfMessageParser;
import io.odpf.depot.utils.MessageConfigUtils;
import io.odpf.stencil.client.ClassLoadStencilClient;
import org.aeonbits.owner.ConfigFactory;
import org.aeonbits.owner.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;

public class BigTableRecordParserTest {

    @Mock
    private ClassLoadStencilClient stencilClient;
    @Mock
    private OdpfMessageSchema schema;
    private BigTableRecordParser bigTableRecordParser;
    private List<OdpfMessage> messages;

    @Before
    public void setUp() throws IOException, InvalidTemplateException {
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "io.odpf.depot.TestBookingLogMessage");
        System.setProperty("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", String.valueOf(SinkConnectorSchemaMessageMode.LOG_MESSAGE));
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "{}");
        System.setProperty("SINK_BIGTABLE_ROW_KEY_TEMPLATE", "row-key-constant-string");


        TestBookingLogKey bookingLogKey1 = TestBookingLogKey.newBuilder().setOrderNumber("order#1").setOrderUrl("order-url#1").build();
        TestBookingLogMessage bookingLogMessage1 = TestBookingLogMessage.newBuilder().setOrderNumber("order#1").setOrderUrl("order-url#1").setServiceType(TestServiceType.Enum.GO_SEND).build();
        TestBookingLogKey bookingLogKey2 = TestBookingLogKey.newBuilder().setOrderNumber("order#2").setOrderUrl("order-url#2").build();
        TestBookingLogMessage bookingLogMessage2 = TestBookingLogMessage.newBuilder().setOrderNumber("order#2").setOrderUrl("order-url#2").setServiceType(TestServiceType.Enum.GO_SHOP).build();

        OdpfMessage message1 = new OdpfMessage(bookingLogKey1.toByteArray(), bookingLogMessage1.toByteArray());
        OdpfMessage message2 = new OdpfMessage(bookingLogKey2.toByteArray(), bookingLogMessage2.toByteArray());
        messages = Collections.list(message1, message2);

        stencilClient = Mockito.mock(ClassLoadStencilClient.class, CALLS_REAL_METHODS);
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(stencilClient);
        BigTableSinkConfig sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils.getModeAndSchema(sinkConfig);
        BigTableSchema bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());

        bigTableRecordParser = new BigTableRecordParser(protoOdpfMessageParser, new BigTableRowKeyParser(new Template(sinkConfig.getRowKeyTemplate()), schema), modeAndSchema, schema, bigtableSchema);
    }

    @Test
    public void shouldReturnValidRecordsForListOfValidOdpfMessages() {
        List<BigTableRecord> records = bigTableRecordParser.convert(messages);
        assertTrue(records.get(0).isValid());
        assertTrue(records.get(1).isValid());
        assertNull(records.get(0).getErrorInfo());
        assertNull(records.get(1).getErrorInfo());
    }

    @Test
    public void shouldReturnInvalidRecordForAnyNullOdpfMessage() {
        List<BigTableRecord> records = bigTableRecordParser.convert(Collections.list(new OdpfMessage(null, null)));
        assertFalse(records.get(0).isValid());
        assertNotNull(records.get(0).getErrorInfo());
    }
}
