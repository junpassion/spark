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

package org.apache.spark.integrationtests.docker

import org.apache.spark.Logging

import scala.collection.mutable
import scala.language.postfixOps
import scala.sys.process._

object Docker extends Logging {
  private val runningDockerContainers = new mutable.HashSet[DockerContainer]()

  def registerContainer(container: DockerContainer) = this.synchronized {
    runningDockerContainers += container
  }

  def killAllLaunchedContainers() = this.synchronized {
    runningDockerContainers.foreach(_.kill())
  }

  def launchContainer(imageTag: String,
                      args: String = "",
                      mountDir: String = ""): DockerContainer = {
    val mountCmd = if (mountDir != "") { " -v " + mountDir } else ""

    val id = new DockerId("docker run --privileged -d %s %s %s".format(mountCmd, imageTag, args).!!)
    try {
      new DockerContainer(id)
    } catch {
      case t: Throwable =>
        kill(id)
        throw t
    }
  }

  def kill(dockerId: DockerId) {
    "docker kill %s".format(dockerId.id).!
  }

  def getLastProcessId: DockerId = {
    var id: String = null
    "docker ps -l -q".!(ProcessLogger(line => id = line))
    new DockerId(id)
  }

  def sparkHomeMountDir: String = {
    val sparkHome = System.getenv("SPARK_HOME")
    assert(sparkHome != null, "Run with a valid SPARK_HOME")

    val containerSparkHome = "/opt/spark"
    "%s:%s".format(sparkHome, containerSparkHome)
  }
}

class DockerId(val id: String) extends AnyVal {
  override def toString: String = id
}
