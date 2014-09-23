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
  private val runningDockerContainers = new mutable.HashSet[DockerId]()

  def registerContainer(containerId: DockerId) = this.synchronized {
    runningDockerContainers += containerId
  }

  def killAllLaunchedContainers() = this.synchronized {
    runningDockerContainers.foreach(kill)
  }

  def launchContainer(imageTag: String,
                      args: String = "",
                      mountDirs: Seq[(String, String)] = Seq.empty): DockerContainer = {
    val mountCmd = mountDirs.map{ case (s, t) => s"-v $s:$t" }.mkString(" ")

    val id = new DockerId("docker run --privileged -d %s %s %s".format(mountCmd, imageTag, args).!!)
    registerContainer(id)
    try {
      new DockerContainer(id)
    } catch {
      case t: Throwable =>
        kill(id)
        throw t
    }
  }

  def kill(dockerId: DockerId) = this.synchronized {
    "docker kill %s".format(dockerId.id).!
    runningDockerContainers -= dockerId
  }

  def getLastProcessId: DockerId = {
    var id: String = null
    "docker ps -l -q".!(ProcessLogger(line => id = line))
    new DockerId(id)
  }

  def dockerHostIp: String = "172.17.42.1" // default docker host ip
}

class DockerId(val id: String) extends AnyVal {
  override def toString: String = id
}
