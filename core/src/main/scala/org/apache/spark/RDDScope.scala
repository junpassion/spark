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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.DynamicVariable
import scala.language.existentials


class RDDScope(val name: String) {

  import RDDScope._

  val parentScope: Option[RDDScope] = currentScope.value

  parentScope.foreach { parent =>
    parent._childScopes += this
  }

  // TODO: capture call site where scope was defined

  private val _childScopes: mutable.ArrayBuffer[RDDScope] = mutable.ArrayBuffer[RDDScope]()

  def childScopes: Seq[RDDScope] = _childScopes

  def define[T](block: => T): T = {
    RDDScope.currentScope.withValue(Some(this)) {
      block
    }
  }

  private def canEqual(other: Any): Boolean = other.isInstanceOf[RDDScope]

  override def equals(other: Any): Boolean = other match {
    case that: RDDScope =>
      (that canEqual this) &&
        parentScope == that.parentScope &&
        name == that.name
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(parentScope, name)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"RDDScope($parentScope, $name)"
}

object RDDScope {
  private[spark] val currentScope: DynamicVariable[Option[RDDScope]] = {
    new DynamicVariable[Option[RDDScope]](None)
  }

  def withScope[T](name: String)(block: => T): T = {
    new RDDScope(name).define {
      block
    }
  }

  def toDot(rdd: RDD[_]): String = {
    // Note: this is a very suboptimal, messy way of doing this graph generation
    case class Edge(from: RDD[_], to: RDD[_])
    val edges = mutable.HashSet[Edge]()
    val visitedRdds = mutable.HashSet[RDD[_]]()
    val scopeToRdds = mutable.Map[RDDScope, mutable.HashSet[RDD[_]]]()
    val rddsWithoutScopes = mutable.HashSet[RDD[_]]()
    def visitRdd(rdd: RDD[_]): Unit = {
      visitedRdds.add(rdd)
      rdd.scope match {
        case Some(scope) =>
          scopeToRdds.getOrElseUpdate(scope, mutable.HashSet()).add(rdd)
        case None =>
          rddsWithoutScopes.add(rdd)
      }
      rdd.dependencies.foreach { dep =>
        edges.add(Edge(dep.rdd, rdd))
        if (!visitedRdds.contains(dep.rdd)) {
          visitRdd(dep.rdd)
        }
      }
    }
    visitRdd(rdd)
    val result = new mutable.StringBuilder()
    val nextClusterId = new AtomicInteger(0)
    // Lay out the subgraphs (this code is a giant mess, beware!)
    result.append("digraph g {\n")
    def addClusterForScope(scope: RDDScope): Unit = {
      result.append(s"subgraph cluster_${nextClusterId.getAndIncrement} {\n")
      result.append(s"""label = "${scope.name}";\n""")
      // Figure out if there are any edges that lie between RDDs defined directly in this scope
      // This is a horribly inefficient way of doing this, but this is just prototype code that I
      // wrote without any serious editing or cleanup
      val rddsDefinedInThisScope = scopeToRdds.getOrElse(scope, Set()).toSet
      // For each RDD, emit an entry so that it's drawn in this scope
      rddsDefinedInThisScope.foreach { rdd =>
        result.append(s"${rdd.id}\n")
      }
      // Find any edges that lie entirely within this scope and draw them
      edges.foreach { case Edge(from, to) =>
        if (from.scope == Some(scope) && to.scope == Some(scope)) {
          result.append(s"${from.id} -> ${to.id};\n")
        }
      }

      scope.childScopes.foreach(addClusterForScope)
      result.append("}\n")
    }
    val orphanScopes: Iterable[RDDScope] = scopeToRdds.keys.filter(_.parentScope.isEmpty)
    orphanScopes.foreach(addClusterForScope)
    // Finally, draw any edges that lie between scopes
    edges.foreach { case Edge(from, to) =>
      if (from.scope != to.scope) {
        result.append(s"${from.id} -> ${to.id};\n")
      }
    }
    // Any labeling of the RDD nodes would be done here:
    visitedRdds.foreach { rdd =>
      result.append(s"""${rdd.id} [label="${rdd.getClass.getSimpleName}(${rdd.id})"];\n""")
    }
    result.append("}\n") // end digraph
    result.toString()
  }

}