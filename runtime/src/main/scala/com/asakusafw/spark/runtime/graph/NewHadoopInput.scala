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
import scala.reflect.{ classTag, ClassTag }

import org.apache.hadoop.mapreduce.{ InputFormat, Job => MRJob }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.spark.backdoor._
import org.apache.spark.util.backdoor._

import com.asakusafw.spark.runtime.Props
import com.asakusafw.spark.runtime.rdd.BranchKey

abstract class NewHadoopInput[IF <: InputFormat[K, V], K, V](
  implicit sc: SparkContext,
  @transient ifClassTag: ClassTag[IF],
  @transient kClassTag: ClassTag[K],
  @transient vClassTag: ClassTag[V])
  extends Input
  with UsingBroadcasts
  with Branching[V] {

  def newJob(rc: RoundContext): MRJob

  override def compute(
    rc: RoundContext)(implicit ec: ExecutionContext): Map[BranchKey, Future[() => RDD[_]]] = {

    val future = zipBroadcasts(rc).map { broadcasts =>

      val job = newJob(rc)

      sc.clearCallSite()
      sc.setCallSite(
        CallSite(rc.roundId.map(r => s"${label}: [${r}]").getOrElse(label), rc.toString))

      val rdd = sc.newAPIHadoopRDD(
        job.getConfiguration,
        classTag[IF].runtimeClass.asInstanceOf[Class[IF]],
        classTag[K].runtimeClass.asInstanceOf[Class[K]],
        classTag[V].runtimeClass.asInstanceOf[Class[V]])

      branch(rdd.asInstanceOf[RDD[(_, V)]], broadcasts, rc.hadoopConf)(
        sc.getConf.getInt(Props.FragmentBufferSize, Props.DefaultFragmentBufferSize))
    }

    branchKeys.map(key => key -> future.map(_(key))).toMap
  }
}
