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

import scala.concurrent.{ ExecutionContext, Future }

import org.apache.spark.{ Partitioner, SparkContext }
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }
import org.apache.spark.rdd.RDD

import org.apache.spark.backdoor._
import org.apache.spark.util.backdoor._

import com.asakusafw.spark.runtime.rdd._

abstract class MapBroadcast(
  prevs: Seq[(Source, BranchKey)],
  sort: Option[SortOrdering],
  group: GroupOrdering,
  part: Partitioner)(
    val label: String)(
      implicit val sc: SparkContext) extends Broadcast {

  override def broadcast(
    rc: RoundContext)(implicit ec: ExecutionContext): Future[Broadcasted[_]] = {

    val rdds = prevs.map {
      case (source, branchKey) =>
        source.getOrCompute(rc).apply(branchKey).map(_().asInstanceOf[RDD[(ShuffleKey, _)]])
    }

    Future.sequence(rdds).map { prevs =>

      sc.clearCallSite()
      sc.setCallSite(
        CallSite(rc.roundId.map(r => s"${label}: [${r}]").getOrElse(label), rc.toString))

      sc.broadcast(
        sc.smcogroup(
          Seq((sc.confluent[ShuffleKey, Any](prevs, part, sort.orElse(Option(group))), sort)),
          part,
          group)
          .map { case (k, vs) => (k.dropOrdering, vs(0).toVector.asInstanceOf[Seq[_]]) }
          .collect()
          .toMap)
    }
  }
}

class MapBroadcastOnce(
  prevs: Seq[(Source, BranchKey)],
  sort: Option[SortOrdering],
  group: GroupOrdering,
  partitioner: Partitioner)(
    label: String)(
      implicit sc: SparkContext)
  extends MapBroadcast(prevs, sort, group, partitioner)(label)
  with BroadcastOnce
