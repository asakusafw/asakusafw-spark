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

import scala.collection.mutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.reflect.{ classTag, ClassTag }

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{ Partitioner, SparkContext }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.compatibility.JobCompatibility
import com.asakusafw.runtime.stage.output.TemporaryOutputFormat
import com.asakusafw.spark.runtime.rdd._

abstract class OutputDriver[T: ClassTag](
  sc: SparkContext,
  hadoopConf: Broadcast[Configuration])(
    @transient prevs: Seq[Future[RDD[(_, T)]]],
    @transient terminators: mutable.Set[Future[Unit]])
  extends SubPlanDriver(sc, hadoopConf) {
  assert(prevs.size > 0,
    s"Previous RDDs should be more than 0: ${prevs.size}")

  val Logger = LoggerFactory.getLogger(getClass())

  override def execute()(
    implicit ec: ExecutionContext): Map[BranchKey, Future[RDD[(ShuffleKey, _)]]] = {
    val job = JobCompatibility.newJob(hadoopConf.value)
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classTag[T].runtimeClass.asInstanceOf[Class[T]])
    job.setOutputFormatClass(classOf[TemporaryOutputFormat[T]])

    val stageInfo = StageInfo.deserialize(job.getConfiguration.get(StageInfo.KEY_NAME))
    TemporaryOutputFormat.setOutputPath(job, new Path(stageInfo.resolveVariables(path)))

    val future = (if (prevs.size == 1) {
      prevs.head
    } else {
      Future.sequence(prevs).map { prevs =>
        val part = Partitioner.defaultPartitioner(prevs.head, prevs.tail: _*)
        val (unioning, coalescing) = prevs.partition(_.partitions.size < part.numPartitions)
        val coalesced = sc.zipPartitions(
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
          sc.union(coalesced +: unioning)
        }
      }
    })
      .map { prev =>

        sc.clearCallSite()
        sc.setCallSite(label)

        val output = prev.map(in => (NullWritable.get, in._2))

        if (Logger.isDebugEnabled()) {
          Logger.debug(output.toDebugString)
        }

        output.saveAsNewAPIHadoopDataset(job.getConfiguration)
      }

    terminators += future

    Map.empty
  }

  def path: String
}
