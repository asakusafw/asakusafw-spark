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

import org.apache.hadoop.mapreduce.{ InputFormat, Job }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.asakusafw.spark.runtime.Props
import com.asakusafw.spark.runtime.driver.Branching
import com.asakusafw.spark.runtime.rdd.BranchKey

abstract class NewHadoopInput[IF <: InputFormat[K, V]: ClassTag, K: ClassTag, V: ClassTag](
  implicit sc: SparkContext)
  extends Input
  with UsingBroadcasts
  with Branching[V] {

  def newJob(rc: RoundContext): Job

  override val dependencies: Set[Node] = broadcasts.values.toSet

  override def compute(
    rc: RoundContext)(implicit ec: ExecutionContext): Map[BranchKey, Future[RDD[_]]] = {

    val future = Future {

      val job = newJob(rc)

      sc.clearCallSite()
      sc.setCallSite(label)

      sc.newAPIHadoopRDD(
        job.getConfiguration,
        classTag[IF].runtimeClass.asInstanceOf[Class[IF]],
        classTag[K].runtimeClass.asInstanceOf[Class[K]],
        classTag[V].runtimeClass.asInstanceOf[Class[V]])

    }.zip(zipBroadcasts(rc)).map {
      case (rdd, broadcasts) =>
        branch(rdd.asInstanceOf[RDD[(_, V)]], broadcasts, rc.hadoopConf)(
          sc.getConf.getInt(Props.FragmentBufferSize, Props.DefaultFragmentBufferSize))
    }

    branchKeys.map(key => key -> future.map(_(key))).toMap
  }
}
