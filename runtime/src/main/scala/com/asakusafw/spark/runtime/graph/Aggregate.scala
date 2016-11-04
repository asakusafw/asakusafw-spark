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
import scala.reflect.ClassTag

import org.apache.spark.{ InterruptibleIterator, Partitioner, TaskContext }
import org.apache.spark.rdd.RDD

import com.asakusafw.spark.runtime.Props
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.rdd._

abstract class Aggregate[V: ClassTag, C: ClassTag](
  @(transient @param) prevs: Seq[(Source, BranchKey)],
  @(transient @param) sort: Option[SortOrdering],
  @(transient @param) partitioner: Partitioner)(
    @transient val broadcasts: Map[BroadcastId, Broadcast[_]])(
      implicit val jobContext: JobContext)
  extends Source
  with UsingBroadcasts
  with Branching[C] {
  self: CacheStrategy[RoundContext, Map[BranchKey, Future[() => RDD[_]]]] =>

  @transient
  private val fragmentBufferSize =
    jobContext.sparkContext.getConf.getInt(
      Props.FragmentBufferSize, Props.DefaultFragmentBufferSize)

  def aggregation: Aggregation[ShuffleKey, V, C]

  override protected def doCompute(
    rc: RoundContext)(implicit ec: ExecutionContext): Map[BranchKey, Future[() => RDD[_]]] = {

    val rdds = prevs.map {
      case (source, branchKey) =>
        source.compute(rc).apply(branchKey).map(_().asInstanceOf[RDD[(ShuffleKey, V)]])
    }

    val future = Future.sequence(rdds).zip(zipBroadcasts(rc)).map {
      case (prevs, broadcasts) =>
        withCallSite(rc) {
          val aggregated = {
            if (aggregation.mapSideCombine) {
              val part = Some(partitioner)
              jobContext.sparkContext.confluent(
                prevs.map { prev =>
                  if (prev.partitioner == part) {
                    prev.asInstanceOf[RDD[(ShuffleKey, C)]]
                  } else {
                    prev.mapPartitions({ iter =>
                      val combiner = aggregation.valueCombiner
                      combiner.insertAll(
                        new ResourceBrokingIterator(
                          rc.hadoopConf.value,
                          iter))
                      val context = TaskContext.get
                      new InterruptibleIterator(context, combiner.iterator)
                    }, preservesPartitioning = true)
                  }
                }, partitioner, sort)
                .mapPartitions({ iter =>
                  val combiner = aggregation.combinerCombiner
                  combiner.insertAll(
                    new ResourceBrokingIterator(
                      rc.hadoopConf.value,
                      iter.map { case (k, v) => (k.dropOrdering, v) }))
                  val context = TaskContext.get
                  new InterruptibleIterator(context, combiner.iterator)
                }, preservesPartitioning = true)
            } else {
              jobContext.sparkContext.confluent(prevs, partitioner, sort)
                .mapPartitions({ iter =>
                  val combiner = aggregation.valueCombiner
                  combiner.insertAll(
                    new ResourceBrokingIterator(
                      rc.hadoopConf.value,
                      iter.map { case (k, v) => (k.dropOrdering, v) }))
                  val context = TaskContext.get
                  new InterruptibleIterator(context, combiner.iterator)
                }, preservesPartitioning = true)
            }
          }

          branch(
            aggregated.asInstanceOf[RDD[(_, C)]],
            broadcasts,
            rc.hadoopConf)(
              fragmentBufferSize)
        }
    }

    branchKeys.map(key => key -> future.map(_(key))).toMap
  }
}
