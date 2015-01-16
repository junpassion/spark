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

import java.io.{FileOutputStream, OutputStream}

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

/**
 * Wraps Hadoop's FSDataOutputStream to address certain Hadoop version incompatibilities.
 */
private[spark]
class HadoopOutputStream private (underlying: FSDataOutputStream) extends OutputStream {

  // The Hadoop APIs have changed over time, so we use reflection to figure out the correct
  // method to use to flush a Hadoop data stream. See SPARK-1518 for details.
  private val hadoopFlushMethod = {
    val cls = classOf[FSDataOutputStream]
    scala.util.Try(cls.getMethod("hflush")).getOrElse(cls.getMethod("sync"))
  }

  override def write(b: Int): Unit = underlying.write(b)

  override def write(b: Array[Byte]): Unit = underlying.write(b)

  override def write(b: Array[Byte], off: Int, len: Int): Unit = underlying.write(b, off, len)

  override def flush(): Unit = hadoopFlushMethod.invoke(underlying)

  override def close(): Unit = underlying.close()
}

private[spark] object HadoopOutputStream {
  def apply(path: Path, fileSystem: FileSystem): OutputStream = {
    val isDefaultLocal = {
      val conf = fileSystem.getConf
      if (conf == null) {
        true
      } else {
        val defaultFs = FileSystem.getDefaultUri(conf).getScheme
        defaultFs == null || defaultFs == "file"
      }
    }
    val scheme = path.toUri.getScheme
    // The Hadoop LocalFileSystem (r1.0.4) has known issues with syncing (HADOOP-7844).
    // Therefore, for local files, use FileOutputStream instead.
    if ((isDefaultLocal && scheme == null) || scheme == "file") {
      new FileOutputStream(path.toUri.getPath, false)  // append = false
    } else {
      new HadoopOutputStream(fileSystem.create(path))
    }
  }
}
