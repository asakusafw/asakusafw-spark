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

import scala.reflect.{ classTag, ClassTag }

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.compatibility.JobCompatibility
import com.asakusafw.runtime.stage.output.TemporaryOutputFormat

abstract class TemporaryOutput[T: ClassTag](
  prevs: Seq[Target])(
    implicit sc: SparkContext)
  extends NewHadoopOutput(prevs) {

  def path: String

  override def newJob(rc: RoundContext): Job = {
    val job = JobCompatibility.newJob(rc.hadoopConf.value)
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classTag[T].runtimeClass.asInstanceOf[Class[T]])
    job.setOutputFormatClass(classOf[TemporaryOutputFormat[T]])

    val stageInfo = StageInfo.deserialize(job.getConfiguration.get(StageInfo.KEY_NAME))
    TemporaryOutputFormat.setOutputPath(job, new Path(stageInfo.resolveUserVariables(path)))

    job
  }
}
