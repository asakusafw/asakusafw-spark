/*
 * Copyright 2011-2016 Asakusa Framework Team.
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
package graph

import scala.annotation.meta.param
import scala.concurrent.{ ExecutionContext, Future }

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import com.asakusafw.spark.runtime.Props
import com.asakusafw.spark.runtime.rdd._

abstract class CoGroup(
  @(transient @param) prevs: Seq[(Seq[(Source, BranchKey)], Option[SortOrdering])],
  @(transient @param) group: GroupOrdering,
  @(transient @param) part: Partitioner)(
    @transient val broadcasts: Map[BroadcastId, Broadcast[_]])(
      implicit val jobContext: JobContext)
  extends Source
  with UsingBroadcasts
  with Branching[IndexedSeq[Iterator[_]]] {
  self: CacheStrategy[RoundContext, Map[BranchKey, Future[RDD[_]]]] =>

  @transient
  private val fragmentBufferSize =
    jobContext.sparkContext.getConf.getInt(
      Props.FragmentBufferSize, Props.DefaultFragmentBufferSize)

  override protected def doCompute(
    rc: RoundContext)(implicit ec: ExecutionContext): Map[BranchKey, Future[RDD[_]]] = {

    val future =
      Future.sequence(
        prevs.map {
          case (targets, sort) =>
            val rdds = targets.map {
              case (source, branchKey) =>
                source.compute(rc).apply(branchKey).map(_.asInstanceOf[RDD[(ShuffleKey, _)]])
            }
            Future.sequence(rdds).map((_, sort))
        }).zip(zipBroadcasts(rc)).map {
          case (prevs, broadcasts) =>
            withCallSite(rc) {
              val cogrouped = jobContext.sparkContext.smcogroup[ShuffleKey](
                prevs.map {
                  case (rdds, sort) =>
                    (jobContext.sparkContext.confluent[ShuffleKey, Any](
                      rdds, part, sort.orElse(Option(group))),
                      sort)
                },
                part,
                group)

              branch(
                cogrouped.asInstanceOf[RDD[(_, IndexedSeq[Iterator[_]])]],
                broadcasts,
                rc.hadoopConf)(
                  fragmentBufferSize)
            }
        }

    branchKeys.map(key => key -> future.map(_(key))).toMap
  }
}
