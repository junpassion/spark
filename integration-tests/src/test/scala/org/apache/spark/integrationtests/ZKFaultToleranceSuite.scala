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

package org.apache.spark.integrationtests


import org.apache.spark.deploy.master.RecoveryState
import org.apache.spark.integrationtests.docker.{ZooKeeperMaster, SparkWorker, SparkMaster, Docker}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.scalatest.{Matchers, BeforeAndAfterEach, FunSuite}
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.Timeouts._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * This suite tests the fault tolerance of the Spark standalone scheduler, mainly the Master.
 * In order to mimic a real distributed cluster more closely, Docker is used.
 * Execute using
 * ./bin/spark-class org.apache.spark.deploy.FaultToleranceTest
 *
 * Make sure that that the environment includes the following properties in SPARK_DAEMON_JAVA_OPTS
 * *and* SPARK_JAVA_OPTS:
 *   - spark.deploy.recoveryMode=ZOOKEEPER
 *   - spark.deploy.zookeeper.url=172.17.42.1:2181
 * Note that 172.17.42.1 is the default docker ip for the host and 2181 is the default ZK port.
 *
 * In case of failure, make sure to kill off prior docker containers before restarting:
 *   docker kill $(docker ps -q)
 *
 * Unfortunately, due to the Docker dependency this suite cannot be run automatically without a
 * working installation of Docker. In addition to having Docker, the following are assumed:
 *   - Docker can run without sudo (see http://docs.docker.io/en/latest/use/basics/)
 *   - The docker images tagged spark-test-master and spark-test-worker are built from the
 *     docker/ directory. Run 'docker/spark-test/build' to generate these.
 */

class ZKFaultToleranceSuite extends FunSuite with BeforeAndAfterEach with Matchers with Logging {

  var cluster: HASparkCluster = _
  var sc: SparkContext = _
  //System.setProperty("spark.driver.host", Docker.dockerHostIp)

  class HASparkCluster {
    val zookeeper: ZooKeeperMaster = new ZooKeeperMaster()
    val conf: SparkConf = new SparkConf()
    conf.set("spark.deploy.recoveryMode", "ZOOKEEPER")
    conf.set("spark.deploy.zookeeper.url", zookeeper.zookeeperUrl)
    conf.set("spark.executor.memory", "256m")
    val masters = ListBuffer[SparkMaster]()
    val workers = ListBuffer[SparkWorker]()

    def addMasters(num: Int) {
      logInfo(s">>>>> ADD MASTERS $num <<<<<")
      (1 to num).foreach { _ => masters += new SparkMaster(conf) }
      masters.foreach(_.waitForUI(10000))
    }

    def addWorkers(num: Int){
      logInfo(s">>>>> ADD WORKERS $num <<<<<")
      val masterUrls = masters.map(_.masterUrl)
      (1 to num).foreach { _ => workers += new SparkWorker(conf, masterUrls) }
      workers.foreach(_.waitForUI(10000))
    }

    def updateState() = {
      masters.foreach(_.updateState())
    }

    def createSparkContext(): SparkContext = {
      // Counter-hack: Because of a hack in SparkEnv#create() that changes this
      // property, we need to reset it.
      System.setProperty("spark.driver.port", "0")
      val masterUrls = "spark://" + masters.map(_.masterUrl.stripPrefix("spark://")).mkString(",")
      new SparkContext(masterUrls, "fault-tolerance", conf)
    }

    def killLeader() {
      logInfo(">>>>> KILL LEADER <<<<<")
      val leader = getLeader()
      masters -= leader
      leader.kill()
    }

    def getLeader(): SparkMaster = {
      val leaders = masters.filter(_.state == RecoveryState.ALIVE)
      assert(leaders.size === 1)
      leaders.head
    }

    def killAll() {
      zookeeper.kill()
      masters.foreach(_.kill())
      workers.foreach(_.kill())
    }
  }

  override def beforeEach() {
    cluster = new HASparkCluster
  }

  override def afterEach() {
    if (sc != null) {
      sc.stop()
      sc = null
    }
    cluster.killAll()
    Docker.killAllLaunchedContainers()
  }

  /**
   * Asserts that the cluster is usable and that the expected masters and workers
   * are all alive in a proper configuration (e.g., only one leader).
   */
  def assertValidClusterState(cluster: HASparkCluster) = {
    logInfo(">>>>> ASSERT VALID CLUSTER STATE <<<<<")

    // Check that the cluster is usable (tests client retry logic, so this may take a long
    // time if the cluster is recovering)
    failAfter(120 seconds) {
      val res = sc.parallelize(0 to 10).collect()
      res.toList should be (0 to 10)
    }

    // Check that the cluster eventually reaches a valid state:
    //eventually (timeout(120 seconds), interval(1 seconds)) {
      cluster.updateState()
      logDebug("Checking for valid cluster state")
      // There should only be one leader
      val (leaders, nonLeaders) = cluster.masters.partition(_.state == RecoveryState.ALIVE)
      leaders.size should be (1)
      // Any master that is not the leader should be in STANDBY mode:
      nonLeaders.map(_.state).toSet should (be (Set()) or be (Set(RecoveryState.STANDBY)))
      // The workers should be alive and registered with the leader:
      cluster.workers.map(_.container.ip).toSet should be (cluster.getLeader().liveWorkerIPs.toSet)
      // At least one application / driver should be alive
      cluster.getLeader().numLiveApps should be >= 1
    //}
  }

  def delay(secs: Duration = 5.seconds) = Thread.sleep(secs.toMillis)

  test("sanity-basic") {
    cluster.addMasters(1)
    cluster.addWorkers(1)
    sc = cluster.createSparkContext()
    assertValidClusterState(cluster)
  }

  test("sanity-many-masters") {
    cluster.addMasters(3)
    cluster.addWorkers(3)
    sc = cluster.createSparkContext()
    assertValidClusterState(cluster)
  }

  test("single-master-halt") {
    cluster.addMasters(3)
    cluster.addWorkers(2)
    sc = cluster.createSparkContext()
    assertValidClusterState(cluster)

    cluster.killLeader()
    delay(30 seconds)
    assertValidClusterState(cluster)
    sc.stop()
    sc = cluster.createSparkContext()
    assertValidClusterState(cluster)
  }

  /*
  test("single-master-restart") {
    addMasters(1)
    addWorkers(2)
    createClient()
    assertValidClusterState()

    killLeader()
    addMasters(1)
    delay(30 seconds)
    assertValidClusterState()

    killLeader()
    addMasters(1)
    delay(30 seconds)
    assertValidClusterState()
  }

  test("cluster-failure") {
    addMasters(2)
    addWorkers(2)
    createClient()
    assertValidClusterState()

    terminateCluster()
    addMasters(2)
    addWorkers(2)
    assertValidClusterState()
  }

  test("all-but-standby-failure") {
    addMasters(2)
    addWorkers(2)
    createClient()
    assertValidClusterState()

    killLeader()
    workers.foreach(_.kill())
    workers.clear()
    delay(30 seconds)
    addWorkers(2)
    assertValidClusterState()
  }

  test("rolling-outage") {
    addMasters(1)
    delay()
    addMasters(1)
    delay()
    addMasters(1)
    addWorkers(2)
    createClient()
    assertValidClusterState()
    assertTrue(getLeader == masters.head)

    (1 to 3).foreach { _ =>
      killLeader()
      delay(30 seconds)
      assertValidClusterState()
      assertTrue(getLeader == masters.head)
      addMasters(1)
    }
  }
*/
}