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

import scala.language.postfixOps
import scala.sys.process._

import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._

class DockerContainer(val id: DockerId) {

  private implicit val formats = DefaultFormats

  private val inspectJson: JValue = parse(s"docker inspect ${id.id}".!!)

  val ip: String = (inspectJson \ "NetworkSettings" \ "IPAddress").extractOpt[String].get

  def getLog(): String = {
    Docker.getLog(this.id)
  }

  def kill() {
    Docker.kill(this.id)
  }
}
