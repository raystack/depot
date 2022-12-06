package io.odpf.depot.message.field.proto;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.odpf.depot.message.field.FieldUtils;
import io.odpf.depot.message.field.GenericField;

public class MessageField implements GenericField {
    private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer()
            .omittingInsignificantWhitespace()
            .preservingProtoFieldNames()
            .includingDefaultValueFields();

    private final Object value;

    public MessageField(Object value) {
        this.value = value;
    }

    @Override
    public String getString() {
        return FieldUtils.convertToStringForMessageTypes(value, this::getMessageString);
    }

    private String getMessageString(Object ob) {
        try {
            return JSON_PRINTER.print((Message) ob);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
