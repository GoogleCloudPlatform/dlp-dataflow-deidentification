/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.swarm.tokenization.parquet;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.commons.lang3.StringUtils.isBlank;
import com.google.privacy.dlp.v2.Table;
import com.google.common.base.Joiner;
import com.google.privacy.dlp.v2.Value;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flattens an AVRO {@link GenericRecord} into a map of flattened JSONPath key and value pairs.
 * Supports record and array type fields, does not support Map field.
 *
 * <p>example: <code>
 * { "name": "John Doe", "contacts": [ { "contact": { "type": "home", "number": "123-456-789" } }, {
 * "contact": { "type": "work", "number": "987-654-321" } }, { "null": null } ] }
 * </code> is converted as <code>
 * { $.name -> {string_value: "John Doe"}, $.contacts[0].contact.type -> {string_value: "home"},
 * $.contacts[0].contact.number -> {string_value: "123-456-789"}, $.contacts[1].contact.type ->
 * {string_value: "work"}, $.contacts[1].contact.number -> {string_value: "987-654-321"} }
 * </code>
 */
public final class GenericRecordFlattener implements RecordFlattener<GenericRecord> {

    public static Logger LOG = LoggerFactory.getLogger(GenericRecordFlattener.class);

    /**
     * Convenience static factory to instantiate a converter for a Generic Record.
     *
     * @param genericRecord the AVRO record to flatten.
     */
    @Override
    public Table.Row flatten(GenericRecord genericRecord) {
        return new TypeFlattener(genericRecord).convert();
    }

    public List<String> flattenColumns(GenericRecord genericRecord) {
        return new TypeFlattener(genericRecord).convertHeaders();
    }

    /** Helper class to actually flatten an AVRO Record. */
    private static final class TypeFlattener {

        private final Schema schema;
        private final GenericRecord genericRecord;
        private final List<Value> valueList;
        private final List<String> flattenedFieldNames;

        private TypeFlattener(Schema schema, GenericRecord genericRecord) {
            this.schema = schema;
            this.genericRecord = genericRecord;
            this.valueList = new ArrayList<>();
            this.flattenedFieldNames = new ArrayList<>();
        }

        private TypeFlattener(GenericRecord genericRecord) {
            this(genericRecord.getSchema(), genericRecord);
        }

        /**
         * Returns a field's key name suffixed with "/bytes" to help distinguish the type in unflatten
         * stage.
         */
        private static String makeByteFieldKey(String fieldKey) {
            return fieldKey + "/bytes";
        }

        private Table.Row convert() {
            convertRecord(genericRecord, schema, null);
            Table.Row.Builder rowBuilder = Table.Row.newBuilder();
            for (Value value:valueList){
                rowBuilder.addValues(value);
            }
            return rowBuilder.build();
        }

        private List<String> convertHeaders() {
            convertRecord(genericRecord, schema, null);
            return flattenedFieldNames;
        }

        /**
         * Flattens the provided value as per the type of item.
         *
         * @param value the object/value to flatten, the type depends on fieldSchema type.
         * @param fieldSchema the schema of the object to be flattened.
         * @param parentKey the flat-field-key of the parent of this field.
         * @param fieldName the name of the field to be flattened.
         */
        private void processType(Object value, Schema fieldSchema, String parentKey, String fieldName) {
//            LOG.info("fieldSchema for {}: {}", fieldName, fieldSchema.getType());
            switch (fieldSchema.getType()) {
                case RECORD:
                    String recordFieldKey = isBlank(fieldName) ? parentKey : String.format("%s.[\"%s\"]", parentKey, fieldName);
                    convertRecord((GenericRecord) value, fieldSchema, recordFieldKey);
                    break;

//                case ARRAY:
//                    processArray(value, fieldSchema, parentKey);
//                    break;

                case ARRAY:
                    List<?> arrayValues = (List<?>) value;
                    List<String> updatedValues = new ArrayList<>();
                    for (int index = 0; index < arrayValues.size(); index++) {
                        updatedValues.add(arrayValues.get(index).toString());
                    }
                    putValue(parentKey, Value.newBuilder().setStringValue(updatedValues.toString()).build());
                    break;

                case UNION:
                    processUnion(value, fieldSchema, parentKey, fieldName);
                    break;

                case ENUM:
                case STRING:
                case MAP:
                    putValue(parentKey, Value.newBuilder().setStringValue(value.toString()).build());
                    break;

                case BOOLEAN:
                    putValue(parentKey, Value.newBuilder().setBooleanValue((boolean) value).build());
                    break;

                case FLOAT:
                    putValue(parentKey, Value.newBuilder().setFloatValue((float) value).build());
                    break;

                case DOUBLE:
                    putValue(parentKey, Value.newBuilder().setFloatValue((double) value).build());
                    break;

                case INT:
                    putValue(parentKey, Value.newBuilder().setIntegerValue((int) value).build());
                    break;

                case LONG:
                    putValue(parentKey, Value.newBuilder().setIntegerValue((long) value).build());
                    break;

                case FIXED:
                    putValue(makeByteFieldKey(parentKey),
                            ByteValueConverter.convertBytesToValue(((GenericFixed) value).bytes()));
                    break;

                case BYTES:
                    putValue(makeByteFieldKey(parentKey),
                            ByteValueConverter.convertBytesToValue(((ByteBuffer) value).array()));
                    break;

                case NULL:
                    putValue(parentKey, Value.newBuilder().getDefaultInstanceForType());
                    break;

                default:
                    throw new IllegalArgumentException("Invalid parquet field type!");
            }
        }

        private void convertRecord(GenericRecord genericRecord, Schema fieldSchema, String parentKey) {
            for (Field field : fieldSchema.getFields()) {
                String fieldName = field.name();
                Object value = genericRecord.get(fieldName);
//                LOG.info("convertRecord.parentKey: {}, convertRecord.fieldName: {}", parentKey, fieldName);
                processType(value, field.schema(), parentKey, fieldName);
            }
        }

        /**
         * Process each element of array further
         * @param value value of the current node
         * @param fieldSchema schema of the current node
         * @param fieldKey parent key or flattened key till current node (including)
         */
        private void processArray(Object value, Schema fieldSchema, String fieldKey) {
            List<?> array = (List<?>) value;
            for (int index = 0; index < array.size(); index++) {
                processType(array.get(index),
                            fieldSchema.getElementType(),
                            String.format("%s[%s]", fieldKey, index),
                            null);
            }
        }

        /**
         * Process Union type field
         * @param value of the current node
         * @param fieldSchema schema of the current node
         * @param parentKey parent key or flattened key till current node (excluding)
         * @param fieldKey field name of current node
         */
        private void processUnion(Object value, Schema fieldSchema, String parentKey, String fieldKey) {
            if (value == null) {
                putValue(Joiner.on(".").skipNulls().join(parentKey, fieldKey), Value.getDefaultInstance());
                return;
            }

            List<Schema> unionTypes = fieldSchema.getTypes();

            checkArgument(unionTypes.size() == 2 && unionTypes.get(0).getType().equals(Schema.Type.NULL),
                          "Only nullable union with one type is supported. found " + unionTypes);

            Schema nonNullType = unionTypes.get(1);
            processType(value,
                        nonNullType,
                        Joiner.on(".").skipNulls().join(parentKey, fieldKey),
                        nonNullType.getFullName());
        }

        /**
         * Assign flattened field names and field values to global variables
         * @param fieldKey final flattened field name for the leaf node
         * @param value value of the leaf node
         */
        private void putValue(String fieldKey, Value value) {
            valueList.add(value);
            flattenedFieldNames.add(fieldKey);
//            LOG.info("flattenedFieldNames: {}", flattenedFieldNames);
        }
    }
}

