/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.unsafe.io;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.spark.unsafe.PlatformDependent;

/**
 * An OutputStream that writes bytes to a size-limited memory region. If a write would go past
 * the end of the region, then an exception is throw, guarding against unsafe memory access errors.
 */
public class SizeLimitedMemoryOutputStream extends OutputStream {

  private Object baseObject;
  private long spaceRemaining;
  private long insertPosition;
  private long bytesWritten;

  public SizeLimitedMemoryOutputStream(Object baseObject, long baseOffset, long sizeLimit) {
    reset(baseObject, baseOffset, sizeLimit);
  }

  public void reset(Object baseObject, long baseOffset, long sizeLimit) {
    assert (sizeLimit >= 0);
    this.baseObject = baseObject;
    spaceRemaining = sizeLimit;
    insertPosition = baseOffset;
    bytesWritten = 0;
  }

  private void ensureSpace(long size) throws IOException{
    if (size > spaceRemaining) {
      throw new IOException("Attempted to write " + size + " bytes but only " + spaceRemaining +
        " bytes of free space are remaining");
    }
  }

  @Override
  public void write(int b) throws IOException {
    ensureSpace(1);
    PlatformDependent.UNSAFE.putByte(baseObject, insertPosition, (byte) b);
    insertPosition += 1;
    spaceRemaining -= 1;
    bytesWritten += 1;
  }

  @Override
  public void write(byte[] bytes) throws IOException {
    write(bytes, 0, bytes.length);
  }


  @Override
  public void write(byte[] bytes, int off, int length) throws IOException {
    ensureSpace(length);
    PlatformDependent.copyMemory(
      bytes,
      PlatformDependent.BYTE_ARRAY_OFFSET + off,
      baseObject,
      insertPosition,
      length);
    insertPosition += length;
    spaceRemaining -= length;
    bytesWritten += length;
  }

  public long bytesWritten() {
    return bytesWritten;
  }

  @Override
  public void flush() throws IOException { }

  @Override
  public void close() throws IOException { }
}
