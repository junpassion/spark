package org.apache.spark.integrationtests.docker

import org.apache.spark.deploy.master.RecoveryState
import org.json4s.jackson.JsonMethods

import scala.io.Source


class SparkMaster {
  private implicit val formats = org.json4s.DefaultFormats

  val container = Docker.launchContainer("spark-test-master", mountDir = Docker.sparkHomeMountDir)

  var state: RecoveryState.Value = _
  var liveWorkerIPs: Seq[String] = _
  var numLiveApps = -1

  def masterUrl: String = s"spark://${container.ip}:7077"

  def waitForUI(timeoutMillis: Int): Unit = {
    val start = System.currentTimeMillis()
    while ((System.currentTimeMillis() - start) < timeoutMillis) {
      try {
        Source.fromURL(s"http://${container.ip}:8080/json")
        return
      } catch {
        case ce: java.net.ConnectException =>
          Thread.sleep(100)
      }
    }
  }

  def updateState(): Unit = {
    val json =
      JsonMethods.parse(Source.fromURL(s"http://${container.ip}:8080/json").bufferedReader())
    val workers = json \ "workers"
    val liveWorkers = workers.children.filter(w => (w \ "state").extract[String] == "ALIVE")
    // Extract the worker IP from "webuiaddress" (rather than "host") because the host name
    // on containers is a weird hash instead of the actual IP address.
    liveWorkerIPs = liveWorkers.map {
      w => (w \ "webuiaddress").extract[String].stripPrefix("http://").stripSuffix(":8081")
    }

    numLiveApps = (json \ "activeapps").children.size

    val status = json \\ "status"
    val stateString = status.extract[String]
    state = RecoveryState.values.filter(state => state.toString == stateString).head
  }

  def kill() {
    container.kill()
  }
}


class SparkWorker(masters: Seq[String]) {
  val container = Docker.launchContainer("spark-test-worker",
    args = masters.mkString(","), mountDir = Docker.sparkHomeMountDir)

  def waitForUI(timeoutMillis: Int): Unit = {
    val start = System.currentTimeMillis()
    while ((System.currentTimeMillis() - start) < timeoutMillis) {
      try {
        Source.fromURL(s"http://${container.ip}:8080/json")
        return
      } catch {
        case ce: java.net.ConnectException =>
          Thread.sleep(100)
      }
    }
  }

  def kill() {
    container.kill()
  }
}