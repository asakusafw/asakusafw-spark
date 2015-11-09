/*
 * Copyright 2011-2015 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.spark.extensions.iterativebatch.runtime
package flow

import scala.collection.mutable
import scala.concurrent.{ ExecutionContext, Future }

import org.apache.spark.SparkContext

class Terminator(
  sinks: Seq[Sink])(
    implicit val sc: SparkContext) extends Sink {

  val label: String = getClass.getName

  val dependencies: Set[Node] = sinks.toSet

  private val nodes: Seq[Node] = {
    val builder = Seq.newBuilder[Node]
    val processed = mutable.Set.empty[Node]

    val stack = mutable.Stack.empty[Node]
    stack.pushAll(sinks.flatMap(_.dependencies))

    def visit(node: Node): Unit = {
      if (!processed(node)) {
        if (node.dependencies.forall(processed)) {
          builder += node
          processed += node
        } else {
          stack.push(node)
          stack.pushAll(node.dependencies.filterNot(processed))
        }
      }
    }

    while (stack.nonEmpty) {
      visit(stack.pop)
    }

    builder.result
  }

  override def submitJob(
    rc: RoundContext)(implicit ec: ExecutionContext): Future[Unit] = {

    nodes.foreach {
      case source: Source =>
        source.getOrCompute(rc)
      case broadcast: Broadcast =>
        broadcast.getOrBroadcast(rc)
    }

    Future.sequence(sinks.map(_.submitJob(rc))).map(_ => ())
  }
}
