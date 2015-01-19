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

import java.io.InputStream

import org.apache.spark.Logging
import org.apache.spark.util.Utils

/**
 * Continuously copies data from the input stream to a file appender.
 *
 * @param inputStream     Input stream to read data from
 * @param fileAppender    File appender to write data to
 * @param streamName      Name to identify the copier (used to name its thread)
 * @param bufferSize      Optional buffer size (used mainly for testing)
 */
private[spark] class StreamFileAppender(
    inputStream: InputStream,
    val fileAppender: FileAppender,
    streamName: String = "UNKNOWN STREAM",
    bufferSize: Int = StreamFileAppender.DEFAULT_BUFFER_SIZE
  ) extends Logging {

  val DEFAULT_BUFFER_SIZE = 8192
  @volatile private var markedForStop = false     // has the copier been asked to stopped
  @volatile private var stopped = false           // has the copier stopped

  // Thread that reads the input stream and writes to the file appender
  private val writingThread = new Thread(s"File appending thread for $streamName") {
    setDaemon(true)
    override def run() {
      Utils.logUncaughtExceptions {
        appendStreamToFile()
      }
    }
  }
  writingThread.start()

  /** Stop the appender */
  def stop(): Unit = {
    markedForStop = true
  }

  /**
   * Wait for the appender to stop appending, either because input stream is closed
   * or because of any error in appending
   */
  def awaitTermination() {
    synchronized {
      if (!stopped) {
        wait()
      }
    }
  }

  /** Continuously read chunks from the input stream and append to the file */
  protected def appendStreamToFile() {
    try {
      logDebug("Started appending thread")
      val buf = new Array[Byte](bufferSize)
      var n = 0
      while (!markedForStop && n != -1) {
        n = inputStream.read(buf)
        if (n != -1) {
          fileAppender.append(buf, 0, n)
        }
      }
    } catch {
      case e: Exception =>
        logError(s"Error writing stream for $streamName", e)
    } finally {
      synchronized {
        stopped = true
        fileAppender.close()
        notifyAll()
      }
    }
  }
}

private[spark] object StreamFileAppender extends Logging {
  val DEFAULT_BUFFER_SIZE = 8192
}
