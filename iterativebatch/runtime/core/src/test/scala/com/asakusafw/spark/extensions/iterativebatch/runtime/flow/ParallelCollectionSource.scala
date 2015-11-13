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
import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.asakusafw.spark.runtime.rdd.BranchKey

class ParallelCollectionSource[T: ClassTag](
  val branchKey: BranchKey,
  data: Seq[T],
  numSlices: Option[Int] = None)(
    val label: String)(
      implicit @transient val sc: SparkContext)
  extends Source with ComputeOnce {

  override val dependencies: Set[Node] = Set.empty

  override def compute(
    rc: RoundContext)(implicit ec: ExecutionContext): Map[BranchKey, Future[RDD[_]]] = {
    val rdd = sc.parallelize(data, numSlices.getOrElse(sc.defaultParallelism))
    Map(branchKey -> Future.successful(rdd))
  }
}
