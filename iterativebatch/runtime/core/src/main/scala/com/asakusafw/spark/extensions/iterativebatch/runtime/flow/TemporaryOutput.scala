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
package com.asakusafw.spark.extensions.iterativebatch.runtime
package flow

import scala.concurrent.{ ExecutionContext, Future }
import scala.reflect.{ classTag, ClassTag }

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{ Partitioner, SparkContext }
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.compatibility.JobCompatibility
import com.asakusafw.runtime.stage.output.TemporaryOutputFormat
import com.asakusafw.spark.runtime.rdd._

abstract class TemporaryOutput[T: ClassTag](
  prevs: Seq[Target])(
    @transient implicit val sc: SparkContext)
  extends Output {

  private[this] val Logger = LoggerFactory.getLogger(getClass())

  def path: String

  override def submitJob(
    rc: RoundContext)(implicit ec: ExecutionContext): Future[Unit] = {

    val rdds = prevs.map {
      case (source, branchKey) =>
        source.getOrCompute(rc).apply(branchKey).map(_.asInstanceOf[RDD[(_, T)]])
    }

    (if (rdds.size == 1) {
      rdds.head
    } else {
      Future.sequence(rdds).map { prevs =>
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

        val job = JobCompatibility.newJob(rc.hadoopConf.value)
        job.setOutputKeyClass(classOf[NullWritable])
        job.setOutputValueClass(classTag[T].runtimeClass.asInstanceOf[Class[T]])
        job.setOutputFormatClass(classOf[TemporaryOutputFormat[T]])

        val stageInfo = StageInfo.deserialize(job.getConfiguration.get(StageInfo.KEY_NAME))
        TemporaryOutputFormat.setOutputPath(job, new Path(stageInfo.resolveUserVariables(path)))

        sc.clearCallSite()
        sc.setCallSite(label)

        val output = prev.map(in => (NullWritable.get, in._2))

        if (Logger.isDebugEnabled()) {
          Logger.debug(output.toDebugString)
        }

        output.saveAsNewAPIHadoopDataset(job.getConfiguration)
      }
  }
}
