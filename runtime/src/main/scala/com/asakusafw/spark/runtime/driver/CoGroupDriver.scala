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
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.spark.runtime.SparkClient.executionContext
import com.asakusafw.spark.runtime.rdd._

abstract class CoGroupDriver(
  sc: SparkContext,
  hadoopConf: Broadcast[Configuration])(
    @transient prevs: Seq[(Seq[Future[RDD[(ShuffleKey, _)]]], Option[Ordering[ShuffleKey]])],
    @transient grouping: Ordering[ShuffleKey],
    @transient part: Partitioner)(
      @transient val broadcasts: Map[BroadcastId, Future[Broadcast[_]]])
  extends SubPlanDriver(sc, hadoopConf) with UsingBroadcasts with Branching[Seq[Iterator[_]]] {
  assert(prevs.size > 0,
    s"Previous RDDs should be more than 0: ${prevs.size}")

  override def execute(): Map[BranchKey, Future[RDD[(ShuffleKey, _)]]] = {
    val future =
      zipBroadcasts().zip((prevs :\ Future.successful(
        List.empty[(Seq[RDD[(ShuffleKey, _)]], Option[Ordering[ShuffleKey]])])) {
          case ((prevs, sort), list) =>
            Future.sequence(prevs).map((_, sort)).zip(list).map {
              case (p, l) => p :: l
            }
        }).map {
        case (broadcasts, prevs) =>

          sc.clearCallSite()
          sc.setCallSite(label)

          val cogrouped = smcogroup[ShuffleKey](
            prevs.map {
              case (rdds, sort) =>
                (confluent[ShuffleKey, Any](rdds, part, sort), sort)
            },
            part,
            grouping)

          branch(cogrouped.asInstanceOf[RDD[(_, Seq[Iterator[_]])]], broadcasts)
      }

    branchKeys.map(key => key -> future.map(_(key))).toMap
  }
}
