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
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.rdd._

abstract class AggregateDriver[V, C](
  sc: SparkContext,
  hadoopConf: Broadcast[Configuration])(
    @transient prevs: Seq[Future[RDD[(ShuffleKey, V)]]],
    @transient sort: Option[Ordering[ShuffleKey]],
    @transient partitioner: Partitioner)(
      @transient val broadcasts: Map[BroadcastId, Future[Broadcast[_]]])
  extends SubPlanDriver(sc, hadoopConf) with UsingBroadcasts with Branching[C] {
  assert(prevs.size > 0,
    s"Previous RDDs should be more than 0: ${prevs.size}")

  override def execute()(
    implicit ec: ExecutionContext): Map[BranchKey, Future[RDD[(ShuffleKey, _)]]] = {

    val future = Future.sequence(prevs).zip(zipBroadcasts()).map {
      case (prevs, broadcasts) =>

        sc.clearCallSite()
        sc.setCallSite(label)

        val aggregated = {
          if (aggregation.mapSideCombine) {
            val part = Some(partitioner)
            sc.confluent(
              prevs.map { prev =>
                if (prev.partitioner == part) {
                  prev.asInstanceOf[RDD[(ShuffleKey, C)]]
                } else {
                  prev.mapPartitions({ iter =>
                    val combiner = aggregation.valueCombiner
                    combiner.insertAll(iter)
                    val context = TaskContext.get
                    new InterruptibleIterator(context, combiner.iterator)
                  }, preservesPartitioning = true)
                }
              }, partitioner, sort)
              .mapPartitions({ iter =>
                val combiner = aggregation.combinerCombiner
                combiner.insertAll(iter.map { case (k, v) => (k.dropOrdering, v) })
                val context = TaskContext.get
                new InterruptibleIterator(context, combiner.iterator)
              }, preservesPartitioning = true)
          } else {
            sc.confluent(prevs, partitioner, sort)
              .mapPartitions({ iter =>
                val combiner = aggregation.valueCombiner
                combiner.insertAll(iter.map { case (k, v) => (k.dropOrdering, v) })
                val context = TaskContext.get
                new InterruptibleIterator(context, combiner.iterator)
              }, preservesPartitioning = true)
          }
        }

        branch(aggregated.asInstanceOf[RDD[(_, C)]], broadcasts, hadoopConf)(
          sc.getConf.getInt(Props.FragmentBufferSize, Props.DefaultFragmentBufferSize))
    }

    branchKeys.map(key => key -> future.map(_(key))).toMap
  }

  def aggregation: Aggregation[ShuffleKey, V, C]
}
