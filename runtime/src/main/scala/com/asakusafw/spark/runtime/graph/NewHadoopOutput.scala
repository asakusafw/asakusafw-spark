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

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{ Job => MRJob }
import org.apache.spark.{ Partitioner, SparkContext }
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import org.apache.spark.backdoor._
import org.apache.spark.util.backdoor._

import com.asakusafw.spark.runtime.rdd._

abstract class NewHadoopOutput(
  prevs: Seq[(Source, BranchKey)])(
    implicit sc: SparkContext)
  extends Output {

  private[this] val Logger = LoggerFactory.getLogger(getClass())

  def newJob(rc: RoundContext): MRJob

  override def submitJob(
    rc: RoundContext)(implicit ec: ExecutionContext): Future[Unit] = {

    val rdds = prevs.map {
      case (source, branchKey) =>
        source.getOrCompute(rc).apply(branchKey).map(_.asInstanceOf[RDD[(_, _)]])
    }

    (if (rdds.size == 1) {
      rdds.head
    } else {
      Future.sequence(rdds).map { prevs =>
        val numPartitions =
          Partitioner.defaultPartitioner(prevs.head, prevs.tail: _*).numPartitions
        val coalesced = prevs.map {
          case prev if prev.partitions.size == numPartitions => prev
          case prev => prev.coalesce(numPartitions, shuffle = false)
        }
        val (zipping, unioning) = coalesced.partition(_.partitions.size == numPartitions)
        val zipped = if (zipping.nonEmpty) {
          zipping.reduceLeft {
            (left, right) =>
              left.zipPartitions(right, preservesPartitioning = false)(_ ++ _)
          }
        } else {
          sc.emptyRDD[(_, _)]
        }
        if (unioning.isEmpty) {
          zipped
        } else {
          sc.union(zipped, unioning: _*)
        }
      }
    })
      .map { prev =>

        val job = newJob(rc)

        sc.clearCallSite()
        sc.setCallSite(
          CallSite(rc.roundId.map(r => s"${label}: [${r}]").getOrElse(label), rc.toString))

        val output = prev.map(in => (NullWritable.get, in._2))

        if (Logger.isTraceEnabled()) {
          Logger.trace(output.toDebugString)
        }

        output.saveAsNewAPIHadoopDataset(job.getConfiguration)
      }
  }
}
