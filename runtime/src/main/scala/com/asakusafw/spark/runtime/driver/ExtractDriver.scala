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
package com.asakusafw.spark.runtime.driver

import scala.concurrent.Future

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{ Partitioner, SparkContext }
import org.apache.spark.rdd.{ RDD, UnionRDD }
import org.apache.spark.broadcast.Broadcast

import com.asakusafw.spark.runtime.SparkClient.executionContext
import com.asakusafw.spark.runtime.rdd._

abstract class ExtractDriver[T](
  sc: SparkContext,
  hadoopConf: Broadcast[Configuration])(
    @transient prevs: Seq[Future[RDD[(_, T)]]])(
      @transient val broadcasts: Map[BroadcastId, Future[Broadcast[_]]])
  extends SubPlanDriver(sc, hadoopConf) with UsingBroadcasts with Branching[T] {
  assert(prevs.size > 0,
    s"Previous RDDs should be more than 0: ${prevs.size}")

  override def execute(): Map[BranchKey, Future[RDD[(ShuffleKey, _)]]] = {

    val future =
      (if (prevs.size == 1) {
        prevs.head
      } else {
        Future.sequence(prevs).map { prevs =>
          val part = Partitioner.defaultPartitioner(prevs.head, prevs.tail: _*)
          val (unioning, coalescing) = prevs.partition(_.partitions.size < part.numPartitions)
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
            new UnionRDD(sc, coalesced +: unioning)
          }
        }
      }).zip(zipBroadcasts()).map {
        case (prev, broadcasts) =>

          sc.clearCallSite()
          sc.setCallSite(label)

          branch(prev, broadcasts)
      }

    branchKeys.map(key => key -> future.map(_(key))).toMap
  }
}
