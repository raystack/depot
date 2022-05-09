package io.odpf.depot.message.proto;

import com.google.protobuf.Descriptors;
import io.odpf.depot.expcetion.ProtoNotFoundException;

import java.util.Map;

public class ProtoFieldParser {
    /**
     * We support nested data type of 15 or less level.
     * We limit the fields to only contain schema upto 15 levels deep
     */
    private static final int MAX_NESTED_SCHEMA_LEVEL = 15;
    private final DescriptorCache descriptorCache = new DescriptorCache();

    public ProtoField parseFields(ProtoField protoField, String protoSchema, Map<String, Descriptors.Descriptor> allDescriptors,
                                  Map<String, String> typeNameToPackageNameMap) {
        return parseFields(protoField, protoSchema, allDescriptors, typeNameToPackageNameMap, 1);
    }

    private ProtoField parseFields(ProtoField protoField, String protoSchema, Map<String, Descriptors.Descriptor> allDescriptors,
                                   Map<String, String> typeNameToPackageNameMap, int level) {

        Descriptors.Descriptor currentProto = descriptorCache.fetch(allDescriptors, typeNameToPackageNameMap, protoSchema);
        if (currentProto == null) {
            throw new ProtoNotFoundException("No Proto found for class " + protoSchema);
        }
        for (Descriptors.FieldDescriptor field : currentProto.getFields()) {
            ProtoField fieldModel = new ProtoField(field.toProto());
            if (fieldModel.isNested()) {
                if (protoSchema.substring(1).equals(currentProto.getFullName())) {
                    if (level >= MAX_NESTED_SCHEMA_LEVEL) {
                        continue;
                    }
                }
                fieldModel = parseFields(fieldModel, field.toProto().getTypeName(), allDescriptors, typeNameToPackageNameMap, level + 1);
            }
            protoField.addField(fieldModel);
        }
        return protoField;
    }
}
