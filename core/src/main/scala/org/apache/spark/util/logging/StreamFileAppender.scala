package org.apache.spark.util.logging

import java.io.{File, InputStream}

import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.util.Utils

/**
 * Continuously appends data from input stream to a file using the given
 * [[org.apache.spark.util.logging.FileAppender]].
 *
 * @param inputStream     Input stream to read data from
 * @param fileAppender    File appender used to write data
 * @param bufferSize      Optional buffer size (used mainly for testing)
 */
private[spark] class StreamFileAppender(
    inputStream: InputStream,
    val fileAppender: FileAppender,
    bufferSize: Int = StreamFileAppender.DEFAULT_BUFFER_SIZE
  ) extends Logging {

  @volatile private var markedForStop = false     // has the appender been asked to stopped
  @volatile private var stopped = false           // has the appender stopped

  // Thread that reads the input stream and writes to file
  private val writingThread = new Thread("File appending thread for " + fileAppender.file) {
    setDaemon(true)
    override def run() {
      Utils.logUncaughtExceptions {
        appendStreamToFile()
      }
    }
  }
  writingThread.start()

  /**Stop the appender */
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
          fileAppender.appendToFile(buf, n)
        }
      }
    } catch {
      case e: Exception =>
        logError(s"Error writing stream to file ${fileAppender.file}", e)
    } finally {
      synchronized {
        stopped = true
        fileAppender.close()
        notifyAll()
      }
    }
  }
}

/**
 * Companion object to [[org.apache.spark.util.logging.StreamFileAppender]] which has helper
 * functions to choose the correct type of FileAppender based on SparkConf configuration.
 */
private[spark] object StreamFileAppender extends Logging {

  val DEFAULT_BUFFER_SIZE = 8192

  /** Create the right stream appender based on Spark configuration */
  def apply(inputStream: InputStream, file: File, conf: SparkConf): StreamFileAppender = {
    new StreamFileAppender(inputStream, FileAppender.apply(file, conf))
  }
}
