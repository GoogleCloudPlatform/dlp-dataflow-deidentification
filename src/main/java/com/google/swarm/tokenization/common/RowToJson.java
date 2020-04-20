package com.google.swarm.tokenization.common;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class RowToJson extends PTransform<PCollection<Row>, PCollection<String>> {

  @Override
  public PCollection<String> expand(PCollection<Row> input) {
    return input.apply(
        "RowToJson",
        ParDo.of(
            new DoFn<Row, String>() {
              Gson json;

              @Setup
              public void setup() {
                json = new Gson();
              }

              @ProcessElement
              public void processElement(ProcessContext c) {
                Row row = c.element();
                JsonObject message = new JsonObject();
                message.addProperty("source_file", row.getString("source_file"));
                message.addProperty("transaction_time", row.getString("transaction_time"));
                message.addProperty("total_bytes_inspected", row.getInt64("total_bytes_inspected"));
                message.addProperty("status", row.getString("status"));
                c.output(json.toJson(message));
              }
            }));
  }
}
