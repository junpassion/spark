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

package org.apache.spark.mllib.rdd

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.random.DistributionGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

import scala.util.Random

private[mllib] class RandomRDDPartition(override val index: Int,
    val size: Int,
    val generator: DistributionGenerator,
    val seed: Long) extends Partition {

  require(size >= 0, "Non-negative partition size required.")
}

// These two classes are necessary since Range objects in Scala cannot have size > Int.MaxValue
private[mllib] class RandomRDD(@transient sc: SparkContext,
    size: Long,
    numPartitions: Int,
    @transient rng: DistributionGenerator,
    @transient seed: Long = Utils.random.nextLong) extends RDD[Double](sc, Nil) {

  require(size > 0, "Positive RDD size required.")
  require(numPartitions > 0, "Positive number of partitions required")
  require(math.ceil(size.toDouble / numPartitions) <= Int.MaxValue,
    "Partition size cannot exceed Int.MaxValue")

  override def compute(splitIn: Partition, context: TaskContext): Iterator[Double] = {
    val split = splitIn.asInstanceOf[RandomRDDPartition]
    RandomRDD.getPointIterator(split)
  }

  override def getPartitions: Array[Partition] = {
    RandomRDD.getPartitions(size, numPartitions, rng, seed)
  }
}

private[mllib] class RandomVectorRDD(@transient sc: SparkContext,
    size: Long,
    vectorSize: Int,
    numPartitions: Int,
    @transient rng: DistributionGenerator,
    @transient seed: Long = Utils.random.nextLong) extends RDD[Vector](sc, Nil) {

  require(size > 0, "Positive RDD size required.")
  require(numPartitions > 0, "Positive number of partitions required")
  require(vectorSize > 0, "Positive vector size required.")
  require(math.ceil(size.toDouble / numPartitions) <= Int.MaxValue,
    "Partition size cannot exceed Int.MaxValue")

  override def compute(splitIn: Partition, context: TaskContext): Iterator[Vector] = {
    val split = splitIn.asInstanceOf[RandomRDDPartition]
    RandomRDD.getVectorIterator(split, vectorSize)
  }

  override protected def getPartitions: Array[Partition] = {
    RandomRDD.getPartitions(size, numPartitions, rng, seed)
  }
}

private[mllib] object RandomRDD {

  def getPartitions(size: Long,
      numPartitions: Int,
      rng: DistributionGenerator,
      seed: Long): Array[Partition] = {

    val partitions = new Array[RandomRDDPartition](numPartitions)
    var i = 0
    var start: Long = 0
    var end: Long = 0
    val random = new Random(seed)
    while (i < numPartitions) {
      end = ((i + 1) * size) / numPartitions
      partitions(i) = new RandomRDDPartition(i, (end - start).toInt, rng, random.nextLong())
      start = end
      i += 1
    }
    partitions.asInstanceOf[Array[Partition]]
  }

  // The RNG has to be reset every time the iterator is requested to guarantee same data
  // every time the content of the RDD is examined.
  def getPointIterator(partition: RandomRDDPartition): Iterator[Double] = {
    val generator = partition.generator.copy()
    generator.setSeed(partition.seed)
    Array.fill(partition.size)(generator.nextValue()).toIterator
  }

  // The RNG has to be reset every time the iterator is requested to guarantee same data
  // every time the content of the RDD is examined.
  def getVectorIterator(partition: RandomRDDPartition, vectorSize: Int): Iterator[Vector] = {
    val generator = partition.generator.copy()
    generator.setSeed(partition.seed)
    Array.fill(partition.size)(new DenseVector(
      (0 until vectorSize).map { _ => generator.nextValue() }.toArray)).toIterator
  }
}
