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

package org.apache.spark.util.logging

import java.io.{InputStream, OutputStream}

import org.apache.spark.Logging
import org.apache.spark.util.Utils

/**
 * Continuously copies data from the input stream to the output stream.
 *
 * @param inputStream     Input stream to read data from
 * @param outputStream    Output stream to write data to
 * @param streamName      Name to identify the copier (used to name its thread)
 * @param bufferSize      Optional buffer size (used mainly for testing)
 */
private[spark] class StreamCopier(
    inputStream: InputStream,
    val outputStream: OutputStream,
    streamName: String = "UNKNOWN STREAM",
    bufferSize: Int = StreamCopier.DEFAULT_BUFFER_SIZE
  ) extends Logging {

  val DEFAULT_BUFFER_SIZE = 8192
  @volatile private var markedForStop = false     // has the copier been asked to stopped
  @volatile private var stopped = false           // has the copier stopped

  // Thread that reads the input stream and writes to file
  private val writingThread = new Thread(s"Stream copying thread for $streamName") {
    setDaemon(true)
    override def run() {
      Utils.logUncaughtExceptions {
        copyStream()
      }
    }
  }
  writingThread.start()

  /** Stop the copier */
  def stop(): Unit = {
    markedForStop = true
  }

  /**
   * Wait for the copying to stop copying, either because input stream is closed
   * or because of any error in copying
   */
  def awaitTermination() {
    synchronized {
      if (!stopped) {
        wait()
      }
    }
  }

  /** Continuously read chunks from the input stream and copy them to the output stream */
  protected def copyStream() {
    try {
      logDebug("Started copying thread")
      val buf = new Array[Byte](bufferSize)
      var n = 0
      while (!markedForStop && n != -1) {
        n = inputStream.read(buf)
        if (n != -1) {
          outputStream.write(buf, 0, n)
        }
      }
    } catch {
      case e: Exception =>
        logError(s"Error copying stream for $streamName", e)
    } finally {
      synchronized {
        stopped = true
        outputStream.close()
        notifyAll()
      }
    }
  }
}

private[spark] object StreamCopier extends Logging {
  val DEFAULT_BUFFER_SIZE = 8192
}
