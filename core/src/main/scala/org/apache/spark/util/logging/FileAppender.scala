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

import java.io.{File, OutputStream}
import java.net.URI

import com.google.common.base.Charsets.UTF_8
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, Path}
import org.apache.spark.util.IntParam
import org.apache.spark.{Logging, SparkConf}

private[spark] class FileAppender(
    pathURI: URI,
    fileSystem: FileSystem,
    outputStreamFactory: (Path, FileSystem) => OutputStream) {

  private val path: Path = new Path(pathURI)
  private var outputStream: OutputStream = null
  // To ensure that the output stream opening function is called at least once, since it may have
  // side-effects (like writing a metadata header or setting file permissions):
  openIfNecessary()

  final def appendLine(string: String): Unit = {
    val bytes = (string + '\n').getBytes(UTF_8)
    append(bytes, 0, bytes.length)
  }

  def append(b: Array[Byte], off: Int, len: Int): Unit = {
    openIfNecessary()
    outputStream.write(b, off, len)
  }

  def close(): Unit = {
    if (outputStream != null) {
      outputStream.close()
      outputStream = null
    }
  }

  def flush(): Unit = {
    if (outputStream != null) {
      outputStream.flush()
    }
  }

  protected def openIfNecessary(): Unit = {
    if (outputStream == null) {
      outputStream = outputStreamFactory(path, fileSystem)
    }
  }
}

/**
 * Helper methods for constructing file appenders based on SparkConf configurations.
 */
private[spark] object FileAppender extends Logging {

  private[spark]
  def DEFAULT_OUTPUT_STREAM_FACTORY(path: Path, fileSystem: FileSystem): OutputStream = {
    HadoopOutputStream(path, fileSystem)
  }

  /** Create the right appender based on Spark configuration */
  def apply(file: File, conf: SparkConf): FileAppender = {
    apply(file.toURI, conf, new LocalFileSystem())
  }

  /** Create the right appender based on Spark configuration */
  def apply(
    file: URI,
    conf: SparkConf,
    fileSystem: FileSystem,
    outputStreamFactory: (Path, FileSystem) => OutputStream = DEFAULT_OUTPUT_STREAM_FACTORY
  ): FileAppender = {

    import org.apache.spark.util.logging.RollingFileAppender._

    val rollingStrategy = conf.get(STRATEGY_PROPERTY, STRATEGY_DEFAULT)
    val rollingSizeBytes = conf.get(SIZE_PROPERTY, STRATEGY_DEFAULT)
    val rollingInterval = conf.get(INTERVAL_PROPERTY, INTERVAL_DEFAULT)

    def createTimeBasedAppender() = {
      val validatedParams: Option[(Long, String)] = rollingInterval match {
        case "daily" =>
          logInfo(s"Rolling executor logs enabled for $file with daily rolling")
          Some(24 * 60 * 60 * 1000L, "--yyyy-MM-dd")
        case "hourly" =>
          logInfo(s"Rolling executor logs enabled for $file with hourly rolling")
          Some(60 * 60 * 1000L, "--yyyy-MM-dd--HH")
        case "minutely" =>
          logInfo(s"Rolling executor logs enabled for $file with rolling every minute")
          Some(60 * 1000L, "--yyyy-MM-dd--HH-mm")
        case IntParam(seconds) =>
          logInfo(s"Rolling executor logs enabled for $file with rolling $seconds seconds")
          Some(seconds * 1000L, "--yyyy-MM-dd--HH-mm-ss")
        case _ =>
          logWarning(s"Illegal interval for rolling executor logs [$rollingInterval], " +
              s"rolling logs not enabled")
          None
      }
      validatedParams.map {
        case (interval, pattern) =>
          new RollingFileAppender(file,
            new TimeBasedRollingPolicy(interval, pattern), conf, fileSystem, outputStreamFactory)
      }.getOrElse {
        new FileAppender(file, fileSystem, outputStreamFactory)
      }
    }

    def createSizeBasedAppender() = {
      rollingSizeBytes match {
        case IntParam(bytes) =>
          logInfo(s"Rolling executor logs enabled for $file with rolling every $bytes bytes")
          new RollingFileAppender(file, new SizeBasedRollingPolicy(bytes), conf, fileSystem, outputStreamFactory)
        case _ =>
          logWarning(
            s"Illegal size [$rollingSizeBytes] for rolling executor logs, rolling logs not enabled")
          new FileAppender(file, fileSystem, outputStreamFactory)
      }
    }

    rollingStrategy match {
      case "" =>
        new FileAppender(file, fileSystem, outputStreamFactory)
      case "time" =>
        createTimeBasedAppender()
      case "size" =>
        createSizeBasedAppender()
      case _ =>
        logWarning(
          s"Illegal strategy [$rollingStrategy] for rolling executor logs, " +
            s"rolling logs not enabled")
        new FileAppender(file, fileSystem, outputStreamFactory)
    }
  }
}


