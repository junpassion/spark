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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.unsafe.memory.MemoryBlock;

public class SizeLimitedMemoryOutputStreamSuite {

  @Test
  public void writingPastSizeLimitShouldThrowException() throws IOException {
    MemoryBlock dataPage = MemoryBlock.fromLongArray(new long[100]);
    OutputStream stream = new SizeLimitedMemoryOutputStream(
      dataPage.getBaseObject(),
      dataPage.getBaseOffset(),
      dataPage.size());
    for (int i = 0; i < dataPage.size(); i++) {
      stream.write(i);
    }
    try {
      stream.write(1);
      Assert.fail("Expected IOException to be thrown.");
    } catch (IOException e) {
      // pass
    }

    stream = new SizeLimitedMemoryOutputStream(
      dataPage.getBaseObject(),
      dataPage.getBaseOffset(),
      dataPage.size());
    try {
      stream.write(new byte[(int) dataPage.size() * 100]);
      Assert.fail("Expected IOException to be thrown.");
    } catch (IOException e) {
      // pass
    }
  }

  @Test
  public void writeWithDataOutputStream() throws IOException {
    long[] longArray = new long[100];
    MemoryBlock dataPage = MemoryBlock.fromLongArray(longArray);
    SizeLimitedMemoryOutputStream stream = new SizeLimitedMemoryOutputStream(
      dataPage.getBaseObject(),
      dataPage.getBaseOffset(),
      dataPage.size());
    DataOutputStream dos = new DataOutputStream(stream);
    for (int i = 0; i < longArray.length; i++) {
      dos.writeLong(i);
    }
    Assert.assertEquals(longArray.length, stream.bytesWritten());
    for (int i = 0; i < longArray.length; i++) {
      Assert.assertEquals(i, Long.reverseBytes(longArray[i]));
    }
  }

  @Test
  public void writeArrayWithOffset() throws IOException {
    long[] longArray = new long[100];
    MemoryBlock dataPage = MemoryBlock.fromLongArray(longArray);
    SizeLimitedMemoryOutputStream stream = new SizeLimitedMemoryOutputStream(
      dataPage.getBaseObject(),
      dataPage.getBaseOffset(),
      dataPage.size());
    byte[] bytes = new byte[16];
    bytes[8] = (byte) 0xFF;
    bytes[9] = (byte) 0xFF;
    bytes[10] = (byte) 0xFF;
    bytes[11] = (byte) 0xFF;
    stream.write(bytes, 8, 4);
    Assert.assertEquals(4, stream.bytesWritten());
    Assert.assertEquals(Integer.MAX_VALUE * 2L + 1, longArray[0]);
  }

}
