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

import org.apache.spark.rdd.RDD
import org.scalatest.{Matchers, FunSuite}

import scala.reflect.ClassTag

class RDDScopeSuite extends FunSuite with Matchers with SharedSparkContext {
  test("RDDs defined at the top level have no scope") {
    val rdd = sc.parallelize(1 to 10).map(identity)
    rdd.scope should be (None)
  }

  test("Basic scope usage") {
    // Example of how library authors can use RDDScope to provide structure / grouping to the
    // RDDs that they define.
    def mapAndFilter[T, R: ClassTag](rdd: RDD[T], mapF: T => R, filterF: R => Boolean): RDD[R] = {
      RDDScope.withScope("mapAndFilter") {
        rdd.map(mapF).filter(filterF)
      }
    }
    val data = sc.parallelize(1 to 10)
    val mappedAndFiltered = mapAndFilter(data, (x: Int) => x * 2, (x: Int) => true)
    // The two intermediate RDDs should be defined in a scope named "mapAndFilter" which has no
    // parent scope:

    val filteredRddScope: RDDScope = mappedAndFiltered.scope.get
    filteredRddScope.name should be ("mapAndFilter")
    filteredRddScope.parentScope should be (None)
    mappedAndFiltered.firstParent.scope.get should be (filteredRddScope)
  }

  test("Nested Scopes") {
    // This demonstrates nesting of scopes, which can occur as progressively higher-level libraries
    // are implemented in terms of lower-level ones
    val data = sc.parallelize(1 to 10)
    val transformedRdd = RDDScope.withScope("outer") {
      val rdd1 = data.map(identity)
      RDDScope.withScope("inner") {
        val x = RDDScope.withScope("filter") {
          rdd1.filter(x => x % 2 == 0)
        }.map(x => (x, x))
        val y = rdd1.map(x => (x, x))
        x.join(y)
      }
    }
    transformedRdd.scope.get.name should be ("inner")
    transformedRdd.scope.get.parentScope.get.name should be ("outer")
    println(RDDScope.toDot(transformedRdd))
  }
}
