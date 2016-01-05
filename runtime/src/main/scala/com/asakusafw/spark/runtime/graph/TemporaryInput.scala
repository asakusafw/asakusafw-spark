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

import scala.reflect.ClassTag

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{ Job => MRJob }
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.SparkContext

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.compatibility.JobCompatibility
import com.asakusafw.runtime.stage.input.TemporaryInputFormat

abstract class TemporaryInput[V: ClassTag](
  @transient val broadcasts: Map[BroadcastId, Broadcast])(
    implicit sc: SparkContext)
  extends NewHadoopInput[TemporaryInputFormat[V], NullWritable, V] {

  def paths: Set[String]

  override def newJob(rc: RoundContext): MRJob = {
    val job = JobCompatibility.newJob(rc.hadoopConf.value)
    val stageInfo = StageInfo.deserialize(job.getConfiguration.get(StageInfo.KEY_NAME))
    FileInputFormat.setInputPaths(job, paths.map { path =>
      new Path(stageInfo.resolveUserVariables(path))
    }.toSeq: _*)
    job
  }
}
