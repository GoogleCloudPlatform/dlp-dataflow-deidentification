package com.google.swarm.tokenization.classification;

import com.google.common.io.Files;
import com.google.swarm.tokenization.common.Util;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SanitizeFileKeyDoFn extends DoFn<FileIO.ReadableFile, KV<FileKeyObject, FileIO.ReadableFile>> {
    public static final Logger LOG = LoggerFactory.getLogger(SanitizeFileKeyDoFn.class);

    // Regular expression that matches valid BQ table IDs
    private static final String TABLE_REGEXP = "[-\\w$@]{1,1024}";
    private Util.InputLocation inputProviderType;

    public SanitizeFileKeyDoFn(Util.InputLocation inputType) {
        this.inputProviderType = inputType;
    }


    public static FileKeyObject sanitizeFileName(String file) {
        String extension = Files.getFileExtension(file);
        if(Util.ALLOWED_TEXT_FILE_EXTENSIONS.contains(extension)){
            extension = "text";
        }
        if (!Util.ALLOWED_FILE_EXTENSIONS.contains(extension) ) {
            throw new RuntimeException(
                    "Invalid file name '"
                            + file
                            + "': must have one of these extensions: "
                            + Util.ALLOWED_FILE_EXTENSIONS);
        }

        String sanitizedName = file.substring(0, file.length() - extension.length() - 1);
        sanitizedName = sanitizedName.replace(".", "_");
        sanitizedName = sanitizedName.replace("-", "_");

        if (!sanitizedName.matches(TABLE_REGEXP)) {
            throw new RuntimeException(
                    "Invalid file name '"
                            + file
                            + "': base name must be a valid BigQuery table name -"
                            + " can contain only letters, numbers, or underscores");
        }

        // Return sanitized file name without extension
//        return new FileKeyObject(sanitizedName,Util.getExtension(extension));
        return new FileKeyObject(sanitizedName,Util.FileType.valueOf(extension.toUpperCase()));
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        FileIO.ReadableFile file = c.element();
        long lastModified = file.getMetadata().lastModifiedMillis();
        if (lastModified == 0L) {
            lastModified = Instant.now().getMillis();
        }
        FileKeyObject fileKey = sanitizeFileName(file.getMetadata().resourceId().getFilename());
        /* For files whose metadata is coming through pub/sub notification and being read,
         * the last modified timestamp would be older than the current window time and hence
         * these files are windowed with timestamp when the file is actually being processed.
         */
        if (this.inputProviderType == Util.InputLocation.GCS)
            c.outputWithTimestamp(KV.of(fileKey, file), Instant.ofEpochMilli(Instant.now().getMillis()));
        else
            c.outputWithTimestamp(KV.of(fileKey, file), Instant.ofEpochMilli(lastModified));
    }
}
