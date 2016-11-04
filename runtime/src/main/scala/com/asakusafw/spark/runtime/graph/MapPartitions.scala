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
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import com.asakusafw.spark.runtime.rdd.BranchKey

abstract class MapPartitions[T, U: ClassTag](
  parent: Source,
  branchKey: BranchKey,
  f: (Int, Iterator[T]) => Iterator[U],
  preservesPartitioning: Boolean = false)(
    implicit val jobContext: JobContext) extends Source with Source.Ops {
  self: CacheStrategy[RoundContext, Map[BranchKey, Future[() => RDD[_]]]] =>

  override val label: String = parent.label

  override def doCompute(
    rc: RoundContext)(implicit ec: ExecutionContext): Map[BranchKey, Future[() => RDD[_]]] = {
    val prevs = parent.compute(rc)
    prevs.updated(
      branchKey,
      prevs(branchKey).map { rddF =>
        () => rddF().asInstanceOf[RDD[T]].mapPartitionsWithIndex(f, preservesPartitioning)
      })
  }
}
