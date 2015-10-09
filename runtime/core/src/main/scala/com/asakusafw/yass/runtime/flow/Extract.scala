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
package com.asakusafw.yass.runtime
package flow

import scala.concurrent.{ ExecutionContext, Future }

import org.apache.spark.{ Partitioner, SparkContext }
import org.apache.spark.rdd.RDD

import com.asakusafw.spark.runtime.driver.BroadcastId
import com.asakusafw.spark.runtime.rdd._

abstract class Extract[T](
  prevs: Seq[Target])(
    val broadcasts: Map[BroadcastId, Broadcast])(
      @transient implicit val sc: SparkContext)
  extends Source
  with UsingBroadcasts
  with Branching[T] {

  override def compute(
    rc: RoundContext)(implicit ec: ExecutionContext): Map[BranchKey, Future[RDD[_]]] = {

    val rdds = prevs.map {
      case (source, branchKey) =>
        source.getOrCompute(rc).apply(branchKey).map(_.asInstanceOf[RDD[(_, T)]])
    }

    val future =
      (if (rdds.size == 1) {
        rdds.head
      } else {
        Future.sequence(rdds).map { rdds =>
          val part = Partitioner.defaultPartitioner(rdds.head, rdds.tail: _*)
          val (unioning, coalescing) = rdds.partition(_.partitions.size < part.numPartitions)
          val coalesced = zipPartitions(
            coalescing.map { prev =>
              if (prev.partitions.size == part.numPartitions) {
                prev
              } else {
                prev.coalesce(part.numPartitions, shuffle = false)
              }
            }, preservesPartitioning = false) {
              _.iterator.flatten.asInstanceOf[Iterator[(_, T)]]
            }
          if (unioning.isEmpty) {
            coalesced
          } else {
            sc.union(coalesced, unioning: _*)
          }
        }
      }).zip(zipBroadcasts(rc)).map {
        case (prev, broadcasts) =>
          branch(prev, broadcasts)(rc)
      }

    branchKeys.map(key => key -> future.map(_(key))).toMap
  }
}
