/*
 * Copyright 2019 Google LLC
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

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextBasedReader {
  public static final Logger LOG = LoggerFactory.getLogger(TextBasedReader.class);
  private static final int READ_BUFFER_SIZE = 524000;
  private final ByteBuffer readBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE);
  private ByteString buffer;
  private int startOfDelimiterInBuffer;
  private int endOfDelimiterInBuffer;
  private long startOfRecord;
  private volatile long startOfNextRecord;
  private volatile boolean eof;
  private volatile boolean elementIsPresent;
  private @Nullable String currentValue;
  private @Nullable SeekableByteChannel inChannel;
  private @Nullable byte[] delimiter;

  public TextBasedReader(SeekableByteChannel channel, long startOfRecord, byte[] delimiter)
      throws IOException {

    buffer = ByteString.EMPTY;
    this.delimiter = delimiter;
    this.inChannel = channel;
    this.startOfRecord = startOfRecord;
    startReading();
  }

  protected long getCurrentOffset() throws NoSuchElementException {
    if (!elementIsPresent) {
      throw new NoSuchElementException();
    }
    return startOfRecord;
  }

  public long getStartOfNextRecord() {
    return this.startOfNextRecord;
  }

  public String getCurrent() throws NoSuchElementException {
    if (!elementIsPresent) {
      throw new NoSuchElementException();
    }
    return currentValue;
  }

  public void startReading() throws IOException {
    long startOffset = this.startOfRecord;
    if (startOffset > 0) {

      long requiredPosition = startOffset - 1;
      if (delimiter != null && startOffset >= delimiter.length) {

        requiredPosition = startOffset - delimiter.length;
      }
      ((SeekableByteChannel) this.inChannel).position(requiredPosition);
      findDelimiterBounds();
      buffer = buffer.substring(endOfDelimiterInBuffer);
      startOfNextRecord = requiredPosition + endOfDelimiterInBuffer;
      endOfDelimiterInBuffer = 0;
      startOfDelimiterInBuffer = 0;
    }
  }

  private void findDelimiterBounds() throws IOException {
    int bytePositionInBuffer = 0;
    while (true) {
      if (!tryToEnsureNumberOfBytesInBuffer(bytePositionInBuffer + 1)) {
        startOfDelimiterInBuffer = endOfDelimiterInBuffer = bytePositionInBuffer;
        break;
      }

      byte currentByte = buffer.byteAt(bytePositionInBuffer);

      if (delimiter == null) {
        // default delimiter
        if (currentByte == '\n') {
          startOfDelimiterInBuffer = bytePositionInBuffer;
          endOfDelimiterInBuffer = startOfDelimiterInBuffer + 1;
          break;
        } else if (currentByte == '\r') {
          startOfDelimiterInBuffer = bytePositionInBuffer;
          endOfDelimiterInBuffer = startOfDelimiterInBuffer + 1;

          if (tryToEnsureNumberOfBytesInBuffer(bytePositionInBuffer + 2)) {
            currentByte = buffer.byteAt(bytePositionInBuffer + 1);
            if (currentByte == '\n') {
              endOfDelimiterInBuffer += 1;
            }
          }
          break;
        }
      } else {
        // user defined delimiter
        int i = 0;
        // initialize delimiter not found
        startOfDelimiterInBuffer = endOfDelimiterInBuffer = bytePositionInBuffer;
        while ((i <= delimiter.length - 1) && (currentByte == delimiter[i])) {
          // read next byte
          i++;
          if (tryToEnsureNumberOfBytesInBuffer(bytePositionInBuffer + i + 1)) {
            currentByte = buffer.byteAt(bytePositionInBuffer + i);
          } else {
            // corner case: delimiter truncated at the end of the file
            startOfDelimiterInBuffer = endOfDelimiterInBuffer = bytePositionInBuffer;
            break;
          }
        }
        if (i == delimiter.length) {
          // all bytes of delimiter found
          endOfDelimiterInBuffer = bytePositionInBuffer + i;
          break;
        }
      }
      // Move to the next byte in buffer.
      bytePositionInBuffer += 1;
    }
  }

  public boolean readNextRecord() throws IOException {
    startOfRecord = startOfNextRecord;
    findDelimiterBounds();

    // If we have reached EOF file and consumed all of the buffer then we know
    // that there are no more records.
    if (eof && buffer.isEmpty()) {
      elementIsPresent = false;
      return false;
    }

    decodeCurrentElement();
    startOfNextRecord = startOfRecord + endOfDelimiterInBuffer;
    return true;
  }

  private void decodeCurrentElement() throws IOException {
    ByteString dataToDecode = buffer.substring(0, startOfDelimiterInBuffer);
    currentValue = dataToDecode.toStringUtf8();
    elementIsPresent = true;
    buffer = buffer.substring(endOfDelimiterInBuffer);
  }

  /** Returns false if we were unable to ensure the minimum capacity by consuming the channel. */
  private boolean tryToEnsureNumberOfBytesInBuffer(int minCapacity) throws IOException {
    // While we aren't at EOF or haven't fulfilled the minimum buffer capacity,
    // attempt to read more bytes.
    while (buffer.size() <= minCapacity && !eof) {
      eof = inChannel.read(readBuffer) == -1;
      readBuffer.flip();
      buffer = buffer.concat(ByteString.copyFrom(readBuffer));
      readBuffer.clear();
    }
    // Return true if we were able to honor the minimum buffer capacity request
    return buffer.size() >= minCapacity;
  }
}
