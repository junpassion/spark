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

package org.apache.spark

import java.io.File

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{SequenceFileOutputFormat, FileOutputFormat, JobConf}
import org.apache.spark.util.Utils
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}


class SparkHadoopWriterSuite extends FunSuite with Matchers with BeforeAndAfterEach {

  private var outputDir: File = null

  override def beforeEach(): Unit = {
    outputDir = Utils.createTempDir()
  }

  override def afterEach(): Unit = {
    if (outputDir != null) {
      Utils.deleteRecursively(outputDir)
    }
  }

  def assertOutputWasWritten(): Unit = {
    val outputFiles = outputDir.listFiles()
    outputFiles.map(_.getName).filter(_.startsWith("part-")).toSet should be (Set("part-00000"))
  }

  def jobConf: JobConf = {
    val conf = new JobConf()
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[Text])
    FileOutputFormat.setOutputPath(conf,
      SparkHadoopWriter.createPathFromString(outputDir.getAbsolutePath, conf))
    conf.set("mapred.output.format.class", classOf[SequenceFileOutputFormat[_, _]].getName)
    conf
  }

  test("basic usage") {
    val driverWriter = new SparkHadoopWriter(jobConf)
    driverWriter.preSetup()
    val executorWriter = Utils.deserialize[SparkHadoopWriter](Utils.serialize(driverWriter))
    assert(!(driverWriter eq executorWriter)) // Checks for reference inequality
    executorWriter.setup(jobid = 1, splitid = 0, attemptid = 0)
    executorWriter.open()
    executorWriter.write(new Text("key"), new Text("value"))
    executorWriter.close()
    executorWriter.commit()
    driverWriter.commitJob()
    assertOutputWasWritten()
  }

  test("speculative task finishes first") {
    val driverWriter = new SparkHadoopWriter(jobConf)
    driverWriter.preSetup()
    // Start the first copy of the task:
    val originalTaskWriter = Utils.deserialize[SparkHadoopWriter](Utils.serialize(driverWriter))
    originalTaskWriter.setup(jobid = 1, splitid = 0, attemptid = 0)
    originalTaskWriter.open()
    originalTaskWriter.write(new Text("key"), new Text("value"))
    // Start a second, speculative copy:
    val speculativeTaskWriter = Utils.deserialize[SparkHadoopWriter](Utils.serialize(driverWriter))
    speculativeTaskWriter.setup(jobid = 1, splitid = 0, attemptid = 1)
    speculativeTaskWriter.open()
    speculativeTaskWriter.write(new Text("key"), new Text("value"))
    // Allow the speculative task to finish first
    speculativeTaskWriter.close()
    speculativeTaskWriter.commit()
    // Now the whole job finishes:
    driverWriter.commitJob()
    // But the original task is still running, so it completes and tries to commit:
    originalTaskWriter.close()
    originalTaskWriter.commit()
    assertOutputWasWritten()
  }

  test("speculative task finishes last") {
    val driverWriter = new SparkHadoopWriter(jobConf)
    driverWriter.preSetup()
    // Start the first copy of the task:
    val originalTaskWriter = Utils.deserialize[SparkHadoopWriter](Utils.serialize(driverWriter))
    originalTaskWriter.setup(jobid = 1, splitid = 0, attemptid = 0)
    originalTaskWriter.open()
    originalTaskWriter.write(new Text("key"), new Text("value"))
    // Start a second, speculative copy:
    val speculativeTaskWriter = Utils.deserialize[SparkHadoopWriter](Utils.serialize(driverWriter))
    speculativeTaskWriter.setup(jobid = 1, splitid = 0, attemptid = 1)
    speculativeTaskWriter.open()
    speculativeTaskWriter.write(new Text("key"), new Text("value"))
    // Allow the original task to finish first
    originalTaskWriter.close()
    originalTaskWriter.commit()
    // Now the whole job finishes:
    driverWriter.commitJob()
    // But the speculative task is still running, so it completes and tries to commit:
    speculativeTaskWriter.close()
    speculativeTaskWriter.commit()
    assertOutputWasWritten()
  }

  test("speculation with interleaved close() and commit() calls") {
    val driverWriter = new SparkHadoopWriter(jobConf)
    driverWriter.preSetup()
    // Start the first copy of the task:
    val originalTaskWriter = Utils.deserialize[SparkHadoopWriter](Utils.serialize(driverWriter))
    originalTaskWriter.setup(jobid = 1, splitid = 0, attemptid = 0)
    originalTaskWriter.open()
    originalTaskWriter.write(new Text("key"), new Text("value"))
    // Start a second, speculative copy:
    val speculativeTaskWriter = Utils.deserialize[SparkHadoopWriter](Utils.serialize(driverWriter))
    speculativeTaskWriter.setup(jobid = 1, splitid = 0, attemptid = 1)
    speculativeTaskWriter.open()
    speculativeTaskWriter.write(new Text("key"), new Text("value"))
    // Interleaved close() and commit() calls:
    originalTaskWriter.close()
    speculativeTaskWriter.close()
    originalTaskWriter.commit()
    speculativeTaskWriter.commit()
    driverWriter.commitJob()
    assertOutputWasWritten()
  }
}
