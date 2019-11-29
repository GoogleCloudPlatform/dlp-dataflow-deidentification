/*
 * Copyright 2018 Google LLC
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
package com.google.swarm.tokenization.common;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

@DefaultCoder(SerializableCoder.class)
public class Row implements Serializable {

  /** */
  private static final long serialVersionUID = 3389828567587870641L;

  private String tableId;
  private String[] header;
  private String[] value;

  public Row(String tableId, String[] header, String[] value) {
    this.tableId = tableId;
    this.header = header;
    this.value = value;
  }

  public String getTableId() {
    return tableId;
  }

  public void setTableId(String tableId) {
    this.tableId = tableId;
  }

  public String[] getHeader() {
    return header;
  }

  public void setHeader(String[] header) {
    this.header = header;
  }

  public String[] getValue() {
    return value;
  }

  public void setValue(String[] value) {
    this.value = value;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(header);
    result = prime * result + ((tableId == null) ? 0 : tableId.hashCode());
    result = prime * result + Arrays.hashCode(value);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    Row other = (Row) obj;
    if (!Arrays.equals(header, other.header)) return false;
    if (tableId == null) {
      if (other.tableId != null) return false;
    } else if (!tableId.equals(other.tableId)) return false;
    if (!Arrays.equals(value, other.value)) return false;
    return true;
  }

  @Override
  public String toString() {
    return "Row [tableId="
        + tableId
        + ", header="
        + Arrays.toString(header)
        + ", value="
        + Arrays.toString(value)
        + "]";
  }
}
