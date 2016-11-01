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
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import com.asakusafw.spark.runtime.JobContext.OutputCounter
import com.asakusafw.spark.runtime.rdd._

abstract class NewHadoopOutput(
  prevs: Seq[(Source, BranchKey)])(
    implicit jobContext: JobContext)
  extends Output {

  private[this] val Logger = LoggerFactory.getLogger(getClass())

  def name: String

  def counter: OutputCounter

  private val statistics = jobContext.getOrNewOutputStatistics(counter, name)

  protected def newJob(rc: RoundContext): MRJob

  override def submitJob(
    rc: RoundContext)(implicit ec: ExecutionContext): Future[Unit] = {

    val rdds = prevs.map {
      case (source, branchKey) =>
        source.compute(rc).apply(branchKey).map(_().asInstanceOf[RDD[(_, _)]])
    }

    (if (rdds.size == 1) {
      rdds.head
    } else {
      Future.sequence(rdds).map { prevs =>
        withCallSite(rc) {
          val zipped =
            prevs.groupBy(_.partitions.length).map {
              case (_, rdds) =>
                rdds.reduce(_.zipPartitions(_, preservesPartitioning = false)(_ ++ _))
            }.toSeq
          if (zipped.size == 1) {
            zipped.head
          } else {
            jobContext.sparkContext.union(zipped)
          }
        }
      }
    })
      .map { prev =>
        withCallSite(rc) {
          val job = newJob(rc)

          val output = prev.map { in =>
            statistics.addRecords(1L)
            (NullWritable.get, in._2)
          }

          if (Logger.isTraceEnabled()) {
            Logger.trace(output.toDebugString)
          }

          output.saveAsNewAPIHadoopDataset(job.getConfiguration)
        }
      }
  }
}
