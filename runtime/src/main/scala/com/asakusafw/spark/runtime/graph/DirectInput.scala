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

import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.hadoop.mapreduce.{ InputFormat, Job => MRJob }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.asakusafw.spark.runtime.rdd.{ BranchKey, ShuffleKey }

abstract class DirectInput[IF <: InputFormat[K, V]: ClassTag, K: ClassTag, V: ClassTag](
  @transient val broadcasts: Map[BroadcastId, Broadcast[_]])(
    implicit sc: SparkContext)
  extends NewHadoopInput[IF, K, V] {
  self: CacheStrategy[RoundContext, Map[BranchKey, Future[RDD[_]]]] =>

  protected def extraConfigurations: Map[String, String]

  override protected def newJob(rc: RoundContext): MRJob = {
    val job = MRJob.getInstance(rc.hadoopConf.value)
    extraConfigurations.foreach {
      case (k, v) => job.getConfiguration.set(k, v)
    }
    job
  }
}
