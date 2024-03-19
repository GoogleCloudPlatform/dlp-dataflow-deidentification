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
package com.google.swarm.tokenization.classification;

import com.google.swarm.tokenization.common.Util;
import java.util.Objects;

public class FileKeyObject {

  private String filename;
  private Util.FileType extension;

  public FileKeyObject(String filename, Util.FileType extension) {
    this.filename = filename;
    this.extension = extension;
  }

  public String getFilename() {
    return filename;
  }

  public void setFilename(String filename) {
    this.filename = filename;
  }

  public Util.FileType getExtension() {
    return extension;
  }

  public void setExtension(Util.FileType extension) {
    this.extension = extension;
  }

  @Override
  public String toString() {
    return "FileKeyObject{" + "filename='" + filename + '\'' + ", extension=" + extension + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof FileKeyObject)) return false;
    FileKeyObject that = (FileKeyObject) o;
    return Objects.equals(filename, that.filename) && extension == that.extension;
  }

  @Override
  public int hashCode() {
    return Objects.hash(filename, extension);
  }
}
