package io.odpf.depot.message.field.proto;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.odpf.depot.message.field.GenericField;

public class MessageField implements GenericField {
    private static JsonFormat.Printer jsonPrinter = JsonFormat.printer()
            .omittingInsignificantWhitespace()
            .preservingProtoFieldNames()
            .includingDefaultValueFields();

    private final DynamicMessage message;

    public MessageField(Object value) {
        this.message = (DynamicMessage) value;
    }

    @Override
    public String getString() {
        try {
            return jsonPrinter.print(message);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
