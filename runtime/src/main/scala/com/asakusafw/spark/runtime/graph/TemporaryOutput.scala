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

import scala.reflect.{ classTag, ClassTag }

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{ Job => MRJob }

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.stage.output.TemporaryOutputFormat
import com.asakusafw.spark.runtime.JobContext.OutputCounter
import com.asakusafw.spark.runtime.rdd.BranchKey

abstract class TemporaryOutput[T: ClassTag](
  prevs: Seq[(Source, BranchKey)])(
    implicit jobContext: JobContext)
  extends NewHadoopOutput(prevs) {

  override def counter: OutputCounter = OutputCounter.External

  protected def path: String

  override protected def newJob(rc: RoundContext): MRJob = {
    val job = MRJob.getInstance(rc.hadoopConf.value)
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classTag[T].runtimeClass.asInstanceOf[Class[T]])
    job.setOutputFormatClass(classOf[TemporaryOutputFormat[T]])

    val stageInfo = StageInfo.deserialize(job.getConfiguration.get(StageInfo.KEY_NAME))
    TemporaryOutputFormat.setOutputPath(
      job,
      new Path(stageInfo.resolveUserVariables(
        s"${path}/${Option(stageInfo.getStageId).getOrElse("-")}")))

    job
  }
}
