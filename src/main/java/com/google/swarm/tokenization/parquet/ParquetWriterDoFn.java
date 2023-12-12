package com.google.swarm.tokenization.parquet;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.function.Consumer;

public class ParquetWriterDoFn extends DoFn<KV<String, Iterable<Table.Row>>, KV<String, GenericRecord>> {

    public static Logger LOG = LoggerFactory.getLogger(ParquetWriterDoFn.class);

    private final String outputBucketName;

    private final PCollectionView<Map<String, Schema>> schemaMapping;

    public ParquetWriterDoFn(
            String outputBucketName, PCollectionView<Map<String, Schema>> schemaMapping) {
        this.outputBucketName = outputBucketName;
        this.schemaMapping = schemaMapping;
    }


    public GenericRecord convertValueToGenericRecord(Object tableRowValue, Schema valueSchema) {
        // if complex schema, make recursive calls and get the value for curr node

        GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(valueSchema);
        switch (valueSchema.getType()) {
            case RECORD:
                Gson gson = new Gson();
                JsonObject json = gson.fromJson((String) tableRowValue, JsonObject.class);
                for (Schema.Field field : valueSchema.getFields()) {
                    // extract individual values for field
                    // TODO: validate the assumption that RECORD fields are valid JSON objects
                    String currentKey = field.name();
                    String currentValue = json.get(currentKey).toString();
                    Object newValue = convertValueToGenericRecord(currentValue, field.schema());
                    genericRecordBuilder.set(field, newValue);
                }
                break;

            case STRING:
            default:
                genericRecordBuilder.set(valueSchema.getName(), (String) tableRowValue);
                break;
        }
        return genericRecordBuilder.build();
    }


    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
        String filename = context.element().getKey();
        Iterable<Table.Row> tableRowIterable = context.element().getValue();
        Map<String, Schema> schemaMappings = context.sideInput(schemaMapping);
        Schema schema = schemaMappings.get(filename);
        String filePath = outputBucketName + "/" + filename + "_" + Instant.now().toString() + ".parquet";

//        List<GenericRecord> genericRecordList = new ArrayList<GenericRecord>();

        Consumer<Table.Row> assignGenericRowValue =
                (Table.Row currRecord) -> {
                    if (currRecord.getValuesCount() != schema.getFields().size()) {
                        throw new RuntimeException(
                                "Size of Table.Row object ("
                                        + currRecord.getValuesCount()
                                        + ") mismatched"
                                        + " with size of Parquet fieldNames ("
                                        + schema.getFields().size()
                                        + ").");
                    }

                    GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);
                    int i = 0;
                    for (Value tableRowValue : currRecord.getValuesList()) {
                        Schema.Field currField = schema.getFields().get(i++);
                        Object newObject = convertValueToGenericRecord(tableRowValue.getStringValue(), currField.schema());
                        genericRecordBuilder.set(currField.name(), newObject);
                    }
                    context.output(KV.of(filename, genericRecordBuilder.build()));
                };

        tableRowIterable.forEach(assignGenericRowValue);
    }
}
