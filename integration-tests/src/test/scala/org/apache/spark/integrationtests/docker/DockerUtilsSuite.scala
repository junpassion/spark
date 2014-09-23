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

import org.apache.curator.framework.CuratorFramework
import org.apache.spark.SparkConf
import org.apache.spark.deploy.master.RecoveryState
import org.scalatest.concurrent.Timeouts
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}

class DockerUtilsSuite extends FunSuite with BeforeAndAfterEach with Matchers with Timeouts {

  override def afterEach(): Unit = {
    Docker.killAllLaunchedContainers()
  }

  test("basic container launching") {
    val container = Docker.launchContainer("ubuntu")
    assert(container.ip !== "")
    container.kill()
  }

  test("basic spark cluster") {
    val conf = new SparkConf()
    // Start a master
    val master = new SparkMaster(conf, Seq.empty)
    master.waitForUI(10000)
    master.updateState()
    assert(master.numLiveApps === 0)
    assert(master.state === RecoveryState.ALIVE)
    assert(master.liveWorkerIPs.isEmpty)

    // Add a worker
    val worker = new SparkWorker(conf, Seq.empty, master.masterUrl)
    worker.waitForUI(10000)
    master.updateState()
    master.liveWorkerIPs should be (Seq(worker.container.ip))

    worker.kill()
    master.kill()
  }

  test("basic zookeeper") {
    val zk = new ZooKeeperMaster()
    var client: CuratorFramework = null
    try {
      client = zk.newCuratorFramework()
      assert(client.getZookeeperClient.blockUntilConnectedOrTimedOut())
    } finally {
      client.close()
    }
  }
}