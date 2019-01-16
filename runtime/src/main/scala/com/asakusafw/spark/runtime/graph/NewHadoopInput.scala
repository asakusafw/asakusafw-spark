/*
 * Copyright 2011-2019 Asakusa Framework Team.
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

import scala.annotation.meta.param
import scala.concurrent.{ ExecutionContext, Future }
import scala.reflect.{ classTag, ClassTag }

import org.apache.hadoop.mapreduce.{ InputFormat, Job => MRJob }
import org.apache.spark.rdd.RDD

import com.asakusafw.spark.runtime.Props
import com.asakusafw.spark.runtime.JobContext.InputCounter
import com.asakusafw.spark.runtime.rdd.BranchKey

abstract class NewHadoopInput[IF <: InputFormat[K, V], K, V](
  implicit jobContext: JobContext,
  @(transient @param) ifClassTag: ClassTag[IF],
  @(transient @param) kClassTag: ClassTag[K],
  @(transient @param) vClassTag: ClassTag[V])
  extends Input
  with UsingBroadcasts
  with Branching[V] {
  self: CacheStrategy[RoundContext, Map[BranchKey, Future[() => RDD[_]]]] =>

  def name: String

  def counter: InputCounter

  private val statistics = jobContext.getOrNewInputStatistics(counter, name)

  protected def newJob(rc: RoundContext): MRJob

  @transient
  private val fragmentBufferSize =
    jobContext.sparkContext.getConf.getInt(
      Props.FragmentBufferSize, Props.DefaultFragmentBufferSize)

  override protected def doCompute(
    rc: RoundContext)(implicit ec: ExecutionContext): Map[BranchKey, Future[() => RDD[_]]] = {

    val future = zipBroadcasts(rc).map { broadcasts =>
      withCallSite(rc) {
        val job = newJob(rc)

        val rdd = jobContext.sparkContext.newAPIHadoopRDD(
          job.getConfiguration,
          classTag[IF].runtimeClass.asInstanceOf[Class[IF]],
          classTag[K].runtimeClass.asInstanceOf[Class[K]],
          classTag[V].runtimeClass.asInstanceOf[Class[V]])
          .map { kv =>
            statistics.addRecords(1L)
            kv
          }

        branch(rdd.asInstanceOf[RDD[(_, V)]], broadcasts, rc.hadoopConf)(fragmentBufferSize)
      }
    }

    branchKeys.map(key => key -> future.map(_(key))).toMap
  }
}
