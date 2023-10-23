/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.swarm.tokenization.orc;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;

/** Create a org.apache.orc.Reader object read/write ORC files */
public class ORCFileReader {

  private static final String FS_GS_IMPL_DEFAULT =
      com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem.class.getName();
  private static final String FS_ABS_GS_IMPL_DEFAULT =
      com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS.class.getName();

  public Reader createORCFileReader(String filePath, String projectId) throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.gs.impl", FS_GS_IMPL_DEFAULT);
    conf.set("fs.AbstractFileSystem.gs.impl", FS_ABS_GS_IMPL_DEFAULT);
    conf.set("fs.gs.project.id", projectId);

    return OrcFile.createReader(new Path(filePath), OrcFile.readerOptions(conf));
  }
}
