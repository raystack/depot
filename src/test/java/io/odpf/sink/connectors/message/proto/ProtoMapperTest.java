package io.odpf.sink.connectors.message.proto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class ProtoMapperTest {

    private final ObjectMapper objectMapper = new ObjectMapper();


    @Test
    public void shouldTestShouldCreateFirstLevelColumnMappingSuccessfully() throws IOException {
        ProtoField protoField = TestProtoUtil.createProtoField(new ArrayList<ProtoField>() {{
            add(TestProtoUtil.createProtoField("order_number", 1));
            add(TestProtoUtil.createProtoField("order_url", 2));
            add(TestProtoUtil.createProtoField("order_details", 3));
            add(TestProtoUtil.createProtoField("created_at", 4));
            add(TestProtoUtil.createProtoField("status", 5));
        }});

        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");
        objNode.put("3", "order_details");
        objNode.put("4", "created_at");
        objNode.put("5", "status");

        String columnMapping = ProtoMapper.generateColumnMappings(protoField.getFields());

        String expectedProtoMapping = objectMapper.writeValueAsString(objNode);
        assertEquals(expectedProtoMapping, columnMapping);
    }

    @Test
    public void shouldTestShouldCreateNestedMapping() throws IOException {
        ProtoField protoField = TestProtoUtil.createProtoField(new ArrayList<ProtoField>() {{
            add(TestProtoUtil.createProtoField("order_number", 1));
            add(TestProtoUtil.createProtoField("order_url", "some.type.name", DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, 2, new ArrayList<ProtoField>() {{
                add(TestProtoUtil.createProtoField("host", 1));
                add(TestProtoUtil.createProtoField("url", 2));
            }}));
            add(TestProtoUtil.createProtoField("order_details", 3));
        }});

        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        ObjectNode innerObjNode = JsonNodeFactory.instance.objectNode();
        innerObjNode.put("1", "host");
        innerObjNode.put("2", "url");
        innerObjNode.put("record_name", "order_url");
        objNode.put("1", "order_number");
        objNode.put("2", innerObjNode);
        objNode.put("3", "order_details");


        String columnMapping = ProtoMapper.generateColumnMappings(protoField.getFields());
        String expectedProtoMapping = objectMapper.writeValueAsString(objNode);
        assertEquals(expectedProtoMapping, columnMapping);
    }

    @Test
    public void generateColumnMappingsForNoFields() throws IOException {
        String protoMapping = ProtoMapper.generateColumnMappings(new ArrayList<>());
        assertEquals(protoMapping, "{}");
    }
}
