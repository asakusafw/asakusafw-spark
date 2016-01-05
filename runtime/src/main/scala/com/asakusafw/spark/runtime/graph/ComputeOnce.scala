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

import scala.collection.mutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import com.asakusafw.spark.runtime.rdd.BranchKey

trait ComputeOnce {
  self: Source =>

  @transient
  private var generatedRDD: Map[BranchKey, Future[RDD[_]]] = _

  override def getOrCompute(
    rc: RoundContext)(implicit ec: ExecutionContext): Map[BranchKey, Future[RDD[_]]] = {
    synchronized {
      if (generatedRDD == null) { // scalastyle:ignore
        generatedRDD = compute(rc)
      }
      generatedRDD
    }
  }
}

object ComputeOnce {

  trait Ops extends Source.Ops {
    self: Source =>

    override def map[T, U: ClassTag](branchKey: BranchKey)(
      f: T => U): Source with Source.Ops = {
      new MapPartitions(this, branchKey, {
        (Int, iter: Iterator[T]) => iter.map(f)
      }, preservesPartitioning = false) with ComputeOnce with Ops
    }

    override def flatMap[T, U: ClassTag](branchKey: BranchKey)(
      f: T => TraversableOnce[U]): Source with Source.Ops = {
      new MapPartitions(this, branchKey, {
        (Int, iter: Iterator[T]) => iter.flatMap(f)
      }, preservesPartitioning = false) with ComputeOnce with Ops
    }

    override def mapPartitions[T, U: ClassTag](branchKey: BranchKey)(
      f: Iterator[T] => Iterator[U],
      preservesPartitioning: Boolean = false): Source with Source.Ops = {
      new MapPartitions(this, branchKey, {
        (index: Int, iter: Iterator[T]) => f(iter)
      }, preservesPartitioning) with ComputeOnce with Ops
    }

    override def mapPartitionsWithIndex[T, U: ClassTag](branchKey: BranchKey)(
      f: (Int, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false): Source with Source.Ops = {
      new MapPartitions(this, branchKey, {
        (index: Int, iter: Iterator[T]) => f(index, iter)
      }, preservesPartitioning) with ComputeOnce with Ops
    }
  }
}
