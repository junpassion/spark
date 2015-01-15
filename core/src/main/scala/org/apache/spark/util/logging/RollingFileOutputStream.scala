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

import java.io.OutputStream
import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.util.logging.RollingFileOutputStream._

/**
 * Writes data to the given file, and rolls over the file after the given interval.
 * The rolled over files are named based on the given pattern.
 *
 * @param activeFileURI           File to write data to
 * @param rollingPolicy           Policy based on which files will be rolled over.
 * @param conf                    SparkConf that is used to pass on extra configurations
 */
private[spark] class RollingFileOutputStream(
    activeFileURI: URI,
    val rollingPolicy: RollingPolicy,
    conf: SparkConf,
    fileSystem: FileSystem
  ) extends OutputStream with Logging {

  private val activeFile = new Path(activeFileURI)
  private val maxRetainedFiles = conf.getInt(RETAINED_FILES_PROPERTY, -1)

  private var fileOutputStream: OutputStream = null

  private def openIfNecessary(): Unit = {
    if (fileOutputStream == null) {
      fileOutputStream = HadoopOutputStream(new Path(activeFileURI), fileSystem)
    }
  }

  private def rolloverIfNecessary(len: Long): Unit = {
    if (rollingPolicy.shouldRollover(len)) {
      rollover()
      rollingPolicy.rolledOver()
    }
  }

  override def write(b: Int): Unit = {
    openIfNecessary()
    rolloverIfNecessary(1)
    fileOutputStream.write(b)
    rollingPolicy.bytesWritten(1)
  }

  override def write(b: Array[Byte]): Unit = {
    openIfNecessary()
    rolloverIfNecessary(b.length)
    fileOutputStream.write(b)
    rollingPolicy.bytesWritten(b.length)
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    openIfNecessary()
    rolloverIfNecessary(len)
    fileOutputStream.write(b, off, len)
    rollingPolicy.bytesWritten(len)
  }

  override def flush(): Unit = {
    if (fileOutputStream != null) {
      fileOutputStream.flush()
    }
  }

  override def close(): Unit = {
    if (fileOutputStream != null) {
      fileOutputStream.close()
      fileOutputStream = null
    }
  }

  /** Rollover the file, by closing the output stream and moving it over */
  private def rollover() {
    try {
      close()
      moveFile()
      openIfNecessary()
      if (maxRetainedFiles > 0) {
        deleteOldFiles()
      }
    } catch {
      case e: Exception =>
        logError(s"Error rolling over $activeFileURI", e)
    }
  }

  /** Move the active log file to a new rollover file */
  private def moveFile() {
    val rolloverSuffix = rollingPolicy.generateRolledOverFileSuffix()
    val rolloverFile = new Path(activeFile.getParent, activeFile.getName)
    try {
      logDebug(s"Attempting to rollover file $activeFileURI to file $rolloverFile")
      if (fileSystem.exists(activeFile)) {
        if (!fileSystem.exists(rolloverFile)) {
          fileSystem.rename(activeFile, rolloverFile)
          logInfo(s"Rolled over $activeFile to $rolloverFile")
        } else {
          // In case the rollover file name clashes, make a unique file name.
          // The resultant file names are long and ugly, so this is used only
          // if there is a name collision. This can be avoided by the using
          // the right pattern such that name collisions do not occur.
          var i = 0
          var altRolloverFile: Path = null
          do {
            altRolloverFile = new Path(activeFile.getParent,
              s"${activeFile.getName}$rolloverSuffix--$i")
            i += 1
          } while (i < 10000 && fileSystem.exists(altRolloverFile))
          logWarning(s"Rollover file $rolloverFile already exists, " +
            s"rolled over $activeFileURI to file $altRolloverFile")
          // TODO: check whether the renaming succeeds:
          fileSystem.rename(activeFile, altRolloverFile)
        }
      } else {
        logWarning(s"File $activeFileURI does not exist")
      }
    }
  }

  /** Retain only last few files */
  private def deleteOldFiles() {
    try {
      val rolledoverFiles =
        getSortedRolledOverFiles(activeFile.getParent, activeFile.getName, fileSystem)
      val filesToBeDeleted = rolledoverFiles.take(
        math.max(0, rolledoverFiles.size - maxRetainedFiles))
      filesToBeDeleted.foreach { file =>
        logInfo(s"Deleting file executor log file $file")
        // TODO: check the return code:
        fileSystem.delete(file, false)
      }
    } catch {
      case e: Exception =>
        logError("Error cleaning logs in directory " + activeFile.getParent, e)
    }
  }
}

/**
 * Companion object to [[org.apache.spark.util.logging.RollingFileOutputStream]]. Defines
 * names of configurations that configure rolling file appenders.
 */
private[spark] object RollingFileOutputStream {
  val STRATEGY_PROPERTY = "spark.executor.logs.rolling.strategy"
  val STRATEGY_DEFAULT = ""
  val INTERVAL_PROPERTY = "spark.executor.logs.rolling.time.interval"
  val INTERVAL_DEFAULT = "daily"
  val SIZE_PROPERTY = "spark.executor.logs.rolling.size.maxBytes"
  val SIZE_DEFAULT = (1024 * 1024).toString
  val RETAINED_FILES_PROPERTY = "spark.executor.logs.rolling.maxRetainedFiles"

  /**
   * Get the sorted list of rolled over files. This assumes that the all the rolled
   * over file names are prefixed with the `activeFileName`, and the active file
   * name has the latest logs. So it sorts all the rolled over logs (that are
   * prefixed with `activeFileName`) and appends the active file
   */
  def getSortedRolledOverFilesPlusActiveFile(
      directory: Path,
      activeFileName: String,
      fileSystem: FileSystem
    ): Seq[Path] = {
    val activeFile = {
      val file = new Path(directory, activeFileName)
      if (fileSystem.exists(file)) Some(file) else None
    }
    getSortedRolledOverFiles(directory, activeFileName, fileSystem) ++ activeFile
  }

  def getSortedRolledOverFiles(
      directory: Path,
      activeFileName: String,
      fileSystem: FileSystem
    ): Seq[Path] = {
    val activeFilePath = new Path(directory, activeFileName)
    val rolledOverFiles = fileSystem.listStatus(directory, new PathFilter {
      override def accept(path: Path): Boolean = {
        val isActiveFile = path.toUri.getPath == activeFilePath.toUri.getPath
        path.getName.startsWith(activeFileName) && !isActiveFile
      }
    }).map(_.getPath).sortBy(_.getName)
    rolledOverFiles
  }
}
