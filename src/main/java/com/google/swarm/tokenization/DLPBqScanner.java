package com.google.swarm.tokenization;

import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions;
import com.google.cloud.bigquery.storage.v1beta1.Storage.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1beta1.TableReferenceProto;
import java.io.IOException;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLPBqScanner {

  public static final Logger LOG = LoggerFactory.getLogger(DLPBqScanner.class);

  public static void main(String[] args) {
    DLPBqScannerOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DLPBqScannerOptions.class);

    run(options);
  }

  static class BigQueryStorageClientFactory {

    static BigQueryStorageClient create() {
      try {
        return BigQueryStorageClient.create();
      } catch (IOException e) {
        LOG.error("Error connecting to BigQueryStorage API: " + e.getMessage());
        throw new RuntimeException(e);
      }
    }
  }

  public interface DLPBqScannerOptions extends PipelineOptions {
    @Description("BigQuery table to export from in the form <project>:<dataset>.<table>")
    @Required
    String getTableRef();

    void setTableRef(String tableRef);

    @Description("Optional: Comma separated list of fields to select from the table.")
    String getFields();

    void setFields(String fields);
  }

  private static PipelineResult run(DLPBqScannerOptions options) {

    Pipeline p = Pipeline.create(options);
    TableReadOptions.Builder builder = TableReadOptions.newBuilder();
    if (options.getFields() != null) {
      builder.addAllSelectedFields(Arrays.asList(options.getFields().split(",\\s*")));
    }
    TableReadOptions tableReadOptions = builder.build();
    BigQueryStorageClient client = BigQueryStorageClientFactory.create();
    ReadSession session =
        ReadSessionFactory.create(client, options.getTableRef(), tableReadOptions);

    // Extract schema from ReadSession
    Schema schema = getTableSchema(session);
    client.close();
    p.apply(
        "ReadFromBigQuery",
        BigQueryIO.read(SchemaAndRecord::getRecord)
            .from(options.getTableRef())
            .withTemplateCompatibility()
            .withMethod(Method.DIRECT_READ)
            .withCoder(AvroCoder.of(schema))
            .withReadOptions(tableReadOptions));

    return p.run();
  }

  private static Schema getTableSchema(ReadSession session) {
    Schema avroSchema;

    avroSchema = new Schema.Parser().parse(session.getAvroSchema().getSchema());
    LOG.info("Schema for export is: " + avroSchema.toString());

    return avroSchema;
  }

  static class ReadSessionFactory {
    static ReadSession create(
        BigQueryStorageClient client, String tableString, TableReadOptions tableReadOptions) {
      TableReference tableReference = BigQueryHelpers.parseTableSpec(tableString);
      String parentProjectId = "projects/" + tableReference.getProjectId();

      TableReferenceProto.TableReference storageTableRef =
          TableReferenceProto.TableReference.newBuilder()
              .setProjectId(tableReference.getProjectId())
              .setDatasetId(tableReference.getDatasetId())
              .setTableId(tableReference.getTableId())
              .build();

      CreateReadSessionRequest.Builder builder =
          CreateReadSessionRequest.newBuilder()
              .setParent(parentProjectId)
              .setReadOptions(tableReadOptions)
              .setTableReference(storageTableRef);
      try {
        return client.createReadSession(builder.build());
      } catch (InvalidArgumentException iae) {
        LOG.error("Error creating ReadSession: " + iae.getMessage());
        throw new RuntimeException(iae);
      }
    }
  }
}
