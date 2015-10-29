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

import scala.reflect.ClassTag

import org.apache.hadoop.mapreduce.{ InputFormat, Job }
import org.apache.spark.SparkContext

import com.asakusafw.runtime.compatibility.JobCompatibility
import com.asakusafw.spark.runtime.driver.BroadcastId

abstract class DirectInput[IF <: InputFormat[K, V]: ClassTag, K: ClassTag, V: ClassTag](
  val broadcasts: Map[BroadcastId, Broadcast])(
    implicit sc: SparkContext)
  extends NewHadoopInput[IF, K, V] {

  def extraConfigurations: Map[String, String]

  override def newJob(rc: RoundContext): Job = {
    val job = JobCompatibility.newJob(rc.hadoopConf.value)
    extraConfigurations.foreach {
      case (k, v) => job.getConfiguration.set(k, v)
    }
    job
  }
}
