package com.google.swarm.tokenization.orc;

import com.google.auto.value.AutoValue;
import com.google.swarm.tokenization.common.ExtractColumnNamesTransform;
import com.google.swarm.tokenization.common.Util;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.orc.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

@AutoValue
@SuppressWarnings("serial")
public abstract class ExtractFileSchemaTransform extends PTransform<PCollection<KV<String, ReadableFile>>,
        PCollectionView<Map<String, String>>> {

    public static final Logger LOG = LoggerFactory.getLogger(ExtractFileSchemaTransform.class);

    public abstract Util.FileType fileType();

    public abstract String projectId();

    public static ExtractFileSchemaTransform.Builder newBuilder() {
        return new AutoValue_ExtractFileSchemaTransform.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

        public abstract Builder setFileType(Util.FileType fileType);

        public abstract Builder setProjectId(String projectId);

        public abstract ExtractFileSchemaTransform build();
    }

    @Override
    public PCollectionView<Map<String, String>>  expand(PCollection<KV<String, ReadableFile>> input) {
        PCollectionView<Map<String, String>> schemaMapping;

        schemaMapping = input
                .apply(ParDo.of(new DoFn<KV<String, ReadableFile>, KV<String, String>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws IOException {
                        String filename = c.element().getKey();
                        ReadableFile randomFile = c.element().getValue();
                        String filePath = randomFile.getMetadata().resourceId().toString();
                        Reader reader = new ORCFileReader().createORCFileReader(filePath, projectId());
                        String schema = reader.getSchema().toString();
                        c.output(KV.of(filename, schema));
                    }
                }))
                .apply("ViewAsList", View.asMap());

        return schemaMapping;
    }
}
