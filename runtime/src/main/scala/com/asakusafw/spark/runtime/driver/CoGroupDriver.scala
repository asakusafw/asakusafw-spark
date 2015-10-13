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
package com.asakusafw.spark.runtime
package driver

import scala.concurrent.{ ExecutionContext, Future }

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{ Partitioner, SparkContext }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

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

  override def execute()(
    implicit ec: ExecutionContext): Map[BranchKey, Future[RDD[(ShuffleKey, _)]]] = {

    val future =
      Future.sequence(
        prevs.map {
          case (rdds, sort) =>
            Future.sequence(rdds).map((_, sort))
        }).zip(zipBroadcasts()).map {
          case (prevs, broadcasts) =>

            sc.clearCallSite()
            sc.setCallSite(label)

            val cogrouped = smcogroup[ShuffleKey](
              prevs.map {
                case (rdds, sort) =>
                  (confluent[ShuffleKey, Any](rdds, part, sort), sort)
              },
              part,
              grouping)

            branch(cogrouped.asInstanceOf[RDD[(_, Seq[Iterator[_]])]], broadcasts, hadoopConf)(
              sc.getConf.getInt(Props.FragmentBufferSize, Props.DefaultFragmentBufferSize))
        }

    branchKeys.map(key => key -> future.map(_(key))).toMap
  }
}
