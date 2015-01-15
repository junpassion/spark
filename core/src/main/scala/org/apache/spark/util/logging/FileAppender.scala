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

import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, Path}

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.util.IntParam

/**
 * Helper methods for constructing file appenders based on SparkConf configurations.
 */
private[spark] object FileAppender extends Logging {

  /** Create the right appender based on Spark configuration */
  def apply(file: File, conf: SparkConf): OutputStream = {
    apply(file.toURI, conf, new LocalFileSystem())
  }

  /** Create the right appender based on Spark configuration */
  def apply(file: URI, conf: SparkConf, fileSystem: FileSystem): OutputStream = {

    import org.apache.spark.util.logging.RollingFileOutputStream._

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
          new RollingFileOutputStream(file,
            new TimeBasedRollingPolicy(interval, pattern), conf, fileSystem)
      }.getOrElse {
        HadoopOutputStream(new Path(file), fileSystem)
      }
    }

    def createSizeBasedAppender() = {
      rollingSizeBytes match {
        case IntParam(bytes) =>
          logInfo(s"Rolling executor logs enabled for $file with rolling every $bytes bytes")
          new RollingFileOutputStream(file, new SizeBasedRollingPolicy(bytes), conf, fileSystem)
        case _ =>
          logWarning(
            s"Illegal size [$rollingSizeBytes] for rolling executor logs, rolling logs not enabled")
          HadoopOutputStream(new Path(file), fileSystem)
      }
    }

    rollingStrategy match {
      case "" =>
        HadoopOutputStream(new Path(file), fileSystem)
      case "time" =>
        createTimeBasedAppender()
      case "size" =>
        createSizeBasedAppender()
      case _ =>
        logWarning(
          s"Illegal strategy [$rollingStrategy] for rolling executor logs, " +
            s"rolling logs not enabled")
        HadoopOutputStream(new Path(file), fileSystem)
    }
  }
}


