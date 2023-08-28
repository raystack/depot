package com.gotocompany.depot.message.proto;

import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.gotocompany.depot.config.SinkConfig;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.spi.json.JsonOrgJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.gotocompany.depot.exception.DeserializerException;
import lombok.AllArgsConstructor;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.JSONWriter;

import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class implements JSONProvider which will be used by JSONPath to query/update input data.
 * This implementation accepts proto message as input and provides a way to operate on ProtoMessage.
 */
public class ProtoJsonProvider implements JsonProvider {

    private static final Long LONG_MASK = 0x00000000FFFFFFFFL;
    private final JsonFormat.Printer printer;
    private static final JsonOrgJsonProvider JSON_P = new JsonOrgJsonProvider();

    public ProtoJsonProvider(SinkConfig sinkConfig) {

        JsonFormat.Printer tempPrinter = JsonFormat.printer()
                .preservingProtoFieldNames()
                .omittingInsignificantWhitespace();
        if (sinkConfig.getSinkDefaultFieldValueEnable()) {
            tempPrinter = tempPrinter.includingDefaultValueFields();
        }
        this.printer = tempPrinter;
    }


    private String printMessage(DynamicMessage msg) {
        try {
            return printer.print(msg);
        } catch (InvalidProtocolBufferException e) {
            String name = msg.getDescriptorForType().getFullName();
            throw new DeserializerException("Unable to convert message to JSON" + name, e);
        }
    }

    private boolean isPrimitive(Descriptors.FieldDescriptor fd) {
        return !fd.getJavaType().equals(Descriptors.FieldDescriptor.JavaType.MESSAGE);
    }

    private Object getPrimitiveValue(Descriptors.FieldDescriptor fd, Object value) {
        switch (fd.getType()) {
            case UINT32:
            case FIXED32:
                return unsignedToString((Integer) value);
            case UINT64:
            case FIXED64:
                return unsignedToString((Long) value);
            case BYTES:
                return BaseEncoding.base64().encode(((ByteString) value).toByteArray());
            case ENUM:
                // Special-case google.protobuf.NullValue (it's an Enum).
                if (fd.getEnumType().getFullName().equals("google.protobuf.NullValue")) {
                    return null;
                }
                return value.toString();
            default:
                return value;
        }
    }

    /**
     * Convert an unsigned 32-bit integer to a string.
     */
    private String unsignedToString(final int value) {
        if (value >= 0) {
            return Integer.toString(value);
        } else {
            return Long.toString(value & LONG_MASK);
        }
    }

    /**
     * Convert an unsigned 64-bit integer to a string.
     */
    private String unsignedToString(final long value) {
        if (value >= 0) {
            return Long.toString(value);
        } else {
            // Pull off the most-significant bit so that BigInteger doesn't think
            // the number is negative, then set it again using setBit().
            return BigInteger.valueOf(value & Long.MAX_VALUE).setBit(Long.SIZE - 1).toString();
        }
    }

    public Object parse(String json) throws InvalidJsonException {
        return JSON_P.parse(json);
    }

    public Object parse(InputStream jsonStream, String charset) throws InvalidJsonException {
        return JSON_P.parse(jsonStream, charset);
    }

    public String toJson(Object obj) {
        if (obj instanceof ProtoFieldValue) {
            return obj.toString();
        } else if (obj instanceof DynamicMessage) {
            return printMessage((DynamicMessage) obj);
        }
        return JSON_P.toJson(obj);
    }

    public Object createArray() {
        return JSON_P.createArray();
    }

    @Override
    public Object createMap() {
        return JSON_P.createMap();
    }

    public boolean isArray(Object obj) {
        if (obj instanceof ProtoFieldValue) {
            return ((ProtoFieldValue) obj).isArray();
        }
        return JSON_P.isArray(obj);
    }

    @Override
    public int length(Object obj) {
        if (obj instanceof DynamicMessage) {
            return ((DynamicMessage) obj).getDescriptorForType().getFields().size();
        } else if (obj instanceof ProtoFieldValue) {
            return ((List) ((ProtoFieldValue) obj).value).size();
        } else if (obj instanceof List) {
            return ((List) obj).size();
        }
        return JSON_P.length(obj);
    }

    @Override
    public Iterable<?> toIterable(Object obj) {
        return (Iterable<?>) obj;
    }

    @Override
    public Collection<String> getPropertyKeys(Object obj) {
        Object val = obj;
        if (obj instanceof ProtoFieldValue) {
            return getPropertyKeys(((ProtoFieldValue) obj).value);
        }
        if (val instanceof DynamicMessage) {
            DynamicMessage msg = (DynamicMessage) val;
            return msg.getAllFields().entrySet().stream().map(fde -> fde.getKey().getName())
                    .collect(Collectors.toList());
        }
        return JSON_P.getPropertyKeys(obj);
    }

    @Override
    public Object getArrayIndex(Object obj, int idx) {
        if (obj instanceof List) {
            return ((List) obj).get(idx);
        } else if (obj instanceof ProtoFieldValue) {
            return ((ProtoFieldValue) obj).getListValue(idx);
        }
        return JSON_P.getArrayIndex(obj, idx);
    }

    @Override
    public Object getArrayIndex(Object obj, int idx, boolean unwrap) {
        return getArrayIndex(obj, idx);
    }

    @Override
    public void setArrayIndex(Object array, int idx, Object newValue) {
        if (array instanceof JSONArray) {
            if (newValue instanceof ProtoFieldValue) {
                JSON_P.setArrayIndex(array, idx, ((ProtoFieldValue) newValue).getJsonValue());
            } else {
                JSON_P.setArrayIndex(array, idx, newValue);
            }
        } else {
            ((List) array).add(idx, newValue);
        }
    }

    @Override
    public void setProperty(Object obj, Object key, Object value) {
        if (value instanceof ProtoFieldValue) {
            JSON_P.setProperty(obj, key, ((ProtoFieldValue) value).getJsonValue());
        } else {
            JSON_P.setProperty(obj, key, value);
        }
    }

    @Override
    public void removeProperty(Object obj, Object key) {
        JSON_P.removeProperty(obj, key);
    }

    @Override
    public boolean isMap(Object obj) {
        if (obj instanceof ProtoFieldValue) {
            return ((ProtoFieldValue) obj).isMap();
        }
        return obj instanceof DynamicMessage || JSON_P.isMap(obj);
    }

    @Override
    public Object unwrap(Object obj) {
        if (obj instanceof ProtoFieldValue) {
            return ((ProtoFieldValue) obj).value;
        }
        return obj;
    }

    @Override
    public Object getMapValue(Object obj, String key) {
        if (obj instanceof DynamicMessage) {
            DynamicMessage msg = (DynamicMessage) obj;
            Descriptors.FieldDescriptor fd = msg.getDescriptorForType().findFieldByName(key);
            if (fd == null) {
                throw new PathNotFoundException(String.format("\"%s\" not found", key));
            }
            Object value = msg.getField(fd);
            return new ProtoFieldValue(fd, value);
        }
        if (obj instanceof ProtoFieldValue) {
            return getMapValue(unwrap(obj), key);
        }
        return JSON_P.getMapValue(obj, key);
    }

    @AllArgsConstructor
    private class ProtoFieldValue implements Iterable {
        private Descriptors.FieldDescriptor fd;
        private Object value;

        private boolean isMap() {
            return fd.getType().equals(Descriptors.FieldDescriptor.Type.MESSAGE) && !isArray();
        }

        private boolean isArray() {
            return value instanceof List;
        }

        private Object getJsonValue() {
            Descriptors.Descriptor parent = fd.getContainingType();
            if (isArray() && ((List) value).isEmpty()) {
                return value;
            }
            if (isPrimitive(fd)) {
                if (isArray()) {
                    return new JSONArray(((List) value).stream().map(a -> getPrimitiveValue(fd, a)).toArray());
                }
                return getPrimitiveValue(fd, value);
            }
            Object newValue = value;
            if (fd.isRepeated() && !isArray()) {
                newValue = new ArrayList() {{
                    add(value);
                }};
            }
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(parent).setField(fd, newValue);
            if (!fd.isRepeated() && !builder.hasField(fd)) {
                return value;
            }
            String jsonValue;
            try {
                jsonValue = printer.print(builder);
            } catch (InvalidProtocolBufferException e) {
                throw new DeserializerException("Unable to get JSON value at " + fd.getFullName(), e);
            }
            JSONObject js = new JSONObject(new JSONTokener(jsonValue));
            if (fd.isRepeated() && !(value instanceof List)) {
                return js.getJSONArray(fd.getName()).get(0);
            }
            return js.get(fd.getName());
        }

        private Object getListValue(int idx) {
            return new ProtoFieldValue(fd, ((List) value).get(idx));
        }

        @Override
        public String toString() {
            return JSONWriter.valueToString(getJsonValue());
        }

        @Override
        public Iterator iterator() {
            return ((List) value).stream().map(o -> new ProtoFieldValue(fd, o)).iterator();
        }
    }

}
