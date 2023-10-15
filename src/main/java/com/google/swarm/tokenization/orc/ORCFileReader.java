package com.google.swarm.tokenization.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;

import java.io.IOException;


/**
 * Create a org.apache.orc.Reader object read/write ORC files
 */
public class ORCFileReader {

    private static final String FS_GS_IMPL_DEFAULT = com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem.class.getName();
    private static final String FS_ABS_GS_IMPL_DEFAULT = com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS.class.getName();

    public Reader createORCFileReader(String filePath, String projectId) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.gs.impl", FS_GS_IMPL_DEFAULT);
        conf.set("fs.AbstractFileSystem.gs.impl", FS_ABS_GS_IMPL_DEFAULT);
        conf.set("fs.gs.project.id", projectId);

        return OrcFile.createReader(new Path(filePath), OrcFile.readerOptions(conf));
    }
}
