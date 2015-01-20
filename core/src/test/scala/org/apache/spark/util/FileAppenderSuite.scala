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

package org.apache.spark.util

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

import scala.collection.mutable.HashSet
import scala.reflect._

import org.scalatest.{BeforeAndAfter, FunSuite}

import com.google.common.base.Charsets.UTF_8
import com.google.common.io.Files

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.util.logging._
import org.apache.spark.util.logging.FileAppender.DEFAULT_OUTPUT_STREAM_FACTORY

class FileAppenderSuite extends FunSuite with BeforeAndAfter with Logging {

  val testFile = new File("FileAppenderSuite-test-" + System.currentTimeMillis).getAbsoluteFile
  val localFileSystem = FileSystem.getLocal(new Configuration())

  before {
    cleanup()
  }

  after {
    cleanup()
  }

  test("basic stream file appender") {
    val testString = (1 to 1000).mkString(", ")
    val inputStream = new ByteArrayInputStream(testString.getBytes(UTF_8))
    val appender = new StreamFileAppender(inputStream, FileAppender(testFile, new SparkConf()))
    inputStream.close()
    appender.awaitTermination()
    assert(Files.toString(testFile, UTF_8) === testString)
  }

  test("rolling file appender - time-based rolling") {
    // setup input stream and appender
    val rolloverIntervalMillis = TimeBasedRollingPolicy.MINIMUM_INTERVAL_SECONDS * 1000L
    val numRollovers = 10
    val textToAppend = (1 to numRollovers).map( _.toString * 10 )

    val clock = new FakeClock()
    val appender = new RollingFileAppender(testFile.toURI,
      new TimeBasedRollingPolicy(rolloverIntervalMillis, s"--HH-mm-ss-SSSS", clock),
      new SparkConf(), localFileSystem, DEFAULT_OUTPUT_STREAM_FACTORY)

    testRolling(appender, textToAppend, rolloverIntervalMillis, clock)
  }

  test("rolling file appender - size-based rolling") {
    // setup input stream and appender
    val rolloverSize = 1000
    val textToAppend = (1 to 3).map( _.toString * rolloverSize )

    val appender = new RollingFileAppender(testFile.toURI,
      new SizeBasedRollingPolicy(rolloverSize, false), new SparkConf(), localFileSystem,
      DEFAULT_OUTPUT_STREAM_FACTORY)

    val files = testRolling(appender, textToAppend, 0, new FakeClock)
    files.foreach { file =>
      val length = localFileSystem.getFileStatus(file).getLen
      logInfo(file.toString + ": " + length + " bytes")
      assert(length <= rolloverSize)
    }
  }

  test("rolling file appender - cleaning") {
    // setup input stream and appender
    val retainedFiles = 10
    val rolloverSize = 1000
    val conf =
      new SparkConf().set(RollingFileAppender.RETAINED_FILES_PROPERTY, retainedFiles.toString)
    val clock = new FakeClock()
    val appender = new RollingFileAppender(testFile.toURI,
      new SizeBasedRollingPolicy(rolloverSize, false, clock), conf, localFileSystem,
      DEFAULT_OUTPUT_STREAM_FACTORY)

    val logDirPath = new Path(testFile.getParent)

    // send data to appender through the input stream, and wait for the data to be written
    val allGeneratedFiles = new HashSet[String]()
    for (item <- (1 to retainedFiles + 2).map { _ => "x" * rolloverSize }) {
      val bytes = item.getBytes(UTF_8)
      appender.append(bytes, 0, bytes.length)
      allGeneratedFiles ++= RollingFileAppender.getSortedRolledOverFilesPlusActiveFile(
        logDirPath, testFile.getName, localFileSystem).map(_.toString)
      clock.advance(10000)
    }
    appender.close()
    logInfo("Appender closed")

    // verify whether the earliest file has been deleted
    val rolledOverFiles = allGeneratedFiles.filter { _ != testFile.toString }.toArray.sorted
    logInfo(s"All rolled over files generated:${rolledOverFiles.size}\n" + rolledOverFiles.mkString("\n"))
    val earliestRolledOverFile = rolledOverFiles.head
    val existingRolledOverFiles = RollingFileAppender.getSortedRolledOverFilesPlusActiveFile(
      logDirPath, testFile.getName, localFileSystem).map(_.toString)
    logInfo("Existing rolled over files:\n" + existingRolledOverFiles.mkString("\n"))
    assert(!existingRolledOverFiles.toSet.contains(earliestRolledOverFile))
    assert(rolledOverFiles.size === retainedFiles + 1)
  }

  test("file appender selection") {
    // Test whether FileAppender.apply() returns the right type of the FileAppender based
    // on SparkConf settings.

    def testAppenderSelection[ExpectedAppender: ClassTag, ExpectedRollingPolicy](
        properties: Seq[(String, String)], expectedRollingPolicyParam: Long = -1): Unit = {

      // Set spark conf properties
      val conf = new SparkConf
      properties.foreach { p =>
        conf.set(p._1, p._2)
      }

      // Create and test file appender
      val appender = FileAppender(testFile, conf)
      //assert(appender.getClass === classTag[ExpectedAppender].getClass)
      assert(appender.getClass.getSimpleName ===
        classTag[ExpectedAppender].runtimeClass.getSimpleName)
      if (appender.isInstanceOf[RollingFileAppender]) {
        val rollingPolicy = appender.asInstanceOf[RollingFileAppender].rollingPolicy
        val policyParam = if (rollingPolicy.isInstanceOf[TimeBasedRollingPolicy]) {
          rollingPolicy.asInstanceOf[TimeBasedRollingPolicy].rolloverIntervalMillis
        } else {
          rollingPolicy.asInstanceOf[SizeBasedRollingPolicy].rolloverSizeBytes
        }
        assert(policyParam === expectedRollingPolicyParam)
      }
      appender.close()
    }

    import RollingFileAppender._

    def rollingStrategy(strategy: String) = Seq(STRATEGY_PROPERTY -> strategy)
    def rollingSize(size: String) = Seq(SIZE_PROPERTY -> size)
    def rollingInterval(interval: String) = Seq(INTERVAL_PROPERTY -> interval)

    val msInDay = 24 * 60 * 60 * 1000L
    val msInHour = 60 * 60 * 1000L
    val msInMinute = 60 * 1000L

    // test no strategy -> no rolling
    testAppenderSelection[FileAppender, Any](Seq.empty)

    // test time based rolling strategy
    testAppenderSelection[RollingFileAppender, Any](rollingStrategy("time"), msInDay)
    testAppenderSelection[RollingFileAppender, TimeBasedRollingPolicy](
      rollingStrategy("time") ++ rollingInterval("daily"), msInDay)
    testAppenderSelection[RollingFileAppender, TimeBasedRollingPolicy](
      rollingStrategy("time") ++ rollingInterval("hourly"), msInHour)
    testAppenderSelection[RollingFileAppender, TimeBasedRollingPolicy](
      rollingStrategy("time") ++ rollingInterval("minutely"), msInMinute)
    testAppenderSelection[RollingFileAppender, TimeBasedRollingPolicy](
      rollingStrategy("time") ++ rollingInterval("123456789"), 123456789 * 1000L)
    testAppenderSelection[FileAppender, Any](
      rollingStrategy("time") ++ rollingInterval("xyz"))

    // test size based rolling strategy
    testAppenderSelection[RollingFileAppender, SizeBasedRollingPolicy](
      rollingStrategy("size") ++ rollingSize("123456789"), 123456789)
    testAppenderSelection[FileAppender, Any](rollingSize("xyz"))

    // test illegal strategy
    testAppenderSelection[FileAppender, Any](rollingStrategy("xyz"))
  }

  /**
   * Run the rolling file appender with data and see whether all the data was written correctly
   * across rolled over files.
   */
  def testRolling(
      appender: FileAppender,
      textToAppend: Seq[String],
      delayBetweenTextsMillis: Long,
      clock: FakeClock
    ): Seq[Path] = {
    // send data to appender through the input stream, and wait for the data to be written
    val expectedText = textToAppend.mkString("")
    for (i <- 0 until textToAppend.size) {
      val bytes = textToAppend(i).getBytes(UTF_8)
      appender.append(bytes, 0, bytes.length)
      clock.advance(delayBetweenTextsMillis)
    }
    logInfo("Data sent to appender")
    appender.close()
    logInfo("Appender closed")

    // verify whether all the data written to rolled over files is same as expected
    val generatedFiles = RollingFileAppender.getSortedRolledOverFilesPlusActiveFile(
      new Path(testFile.getParentFile.toURI), testFile.getName, localFileSystem)
    logInfo("Filtered files: \n" + generatedFiles.mkString("\n"))
    assert(generatedFiles.size > 1)
    val allText = generatedFiles.map { file =>
      Files.toString(new File(file.toUri), UTF_8)
    }.mkString("")
    assert(allText === expectedText)
    generatedFiles
  }

  /** Delete all the generated rolledover files */
  def cleanup() {
    testFile.getParentFile.listFiles.filter { file =>
      file.getName.startsWith(testFile.getName)
    }.foreach { _.delete() }
  }
}
