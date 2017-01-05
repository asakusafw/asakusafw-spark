/*
 * Copyright 2011-2017 Asakusa Framework Team.
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
package graph

import scala.collection.mutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import com.asakusafw.spark.runtime.RoundContext
import com.asakusafw.spark.runtime.graph.{ CacheOnce, MapPartitions, Source }
import com.asakusafw.spark.runtime.rdd.BranchKey

object RoundAwareCacheOnce {

  trait Ops extends RoundAwareSource.Ops {
    self: Source =>

    override def map[T, U: ClassTag](branchKey: BranchKey)(
      f: T => U): Source with RoundAwareSource.Ops = {
      new MapPartitions(this, branchKey, {
        (Int, iter: Iterator[T]) => iter.map(f)
      }, preservesPartitioning = false) with CacheOnce[RoundContext, Map[BranchKey, Future[() => RDD[_]]]] with Ops // scalastyle:ignore
    }

    override def mapWithRoundContext[T, U: ClassTag](branchKey: BranchKey)(
      f: RoundContext => T => U): Source with RoundAwareSource.Ops = {
      new RoundAwareMapPartitions(this, branchKey, {
        rc: RoundContext => (Int, iter: Iterator[T]) => iter.map(f(rc))
      }, preservesPartitioning = false) with CacheAlways[RoundContext, Map[BranchKey, Future[() => RDD[_]]]] with CacheAlways.Ops // scalastyle:ignore
    }

    override def flatMap[T, U: ClassTag](branchKey: BranchKey)(
      f: T => TraversableOnce[U]): Source with RoundAwareSource.Ops = {
      new MapPartitions(this, branchKey, {
        (Int, iter: Iterator[T]) => iter.flatMap(f)
      }, preservesPartitioning = false) with CacheOnce[RoundContext, Map[BranchKey, Future[() => RDD[_]]]] with Ops // scalastyle:ignore
    }

    override def flatMapWithRoundContext[T, U: ClassTag](branchKey: BranchKey)(
      f: RoundContext => T => TraversableOnce[U]): Source with RoundAwareSource.Ops = {
      new RoundAwareMapPartitions(this, branchKey, {
        rc: RoundContext => (Int, iter: Iterator[T]) => iter.flatMap(f(rc))
      }, preservesPartitioning = false) with CacheAlways[RoundContext, Map[BranchKey, Future[() => RDD[_]]]] with CacheAlways.Ops // scalastyle:ignore
    }

    override def mapPartitions[T, U: ClassTag](branchKey: BranchKey)(
      f: Iterator[T] => Iterator[U],
      preservesPartitioning: Boolean = false): Source with RoundAwareSource.Ops = {
      new MapPartitions(this, branchKey, {
        (Int, iter: Iterator[T]) => f(iter)
      }, preservesPartitioning) with CacheOnce[RoundContext, Map[BranchKey, Future[() => RDD[_]]]] with Ops // scalastyle:ignore
    }

    override def mapPartitionsWithRoundContext[T, U: ClassTag](branchKey: BranchKey)(
      f: RoundContext => Iterator[T] => Iterator[U],
      preservesPartitioning: Boolean = false): Source with RoundAwareSource.Ops = {
      new RoundAwareMapPartitions(this, branchKey, {
        rc: RoundContext => (Int, iter: Iterator[T]) => f(rc)(iter)
      }, preservesPartitioning) with CacheAlways[RoundContext, Map[BranchKey, Future[() => RDD[_]]]] with CacheAlways.Ops // scalastyle:ignore
    }

    override def mapPartitionsWithIndex[T, U: ClassTag](branchKey: BranchKey)(
      f: (Int, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false): Source with RoundAwareSource.Ops = {
      new MapPartitions(this, branchKey, {
        (index: Int, iter: Iterator[T]) => f(index, iter)
      }, preservesPartitioning) with CacheOnce[RoundContext, Map[BranchKey, Future[() => RDD[_]]]] with Ops // scalastyle:ignore
    }

    override def mapPartitionsWithIndexAndRoundContext[T, U: ClassTag](branchKey: BranchKey)(
      f: RoundContext => (Int, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false): Source with RoundAwareSource.Ops = {
      new RoundAwareMapPartitions(this, branchKey, {
        rc: RoundContext => (index: Int, iter: Iterator[T]) => f(rc)(index, iter)
      }, preservesPartitioning) with CacheAlways[RoundContext, Map[BranchKey, Future[() => RDD[_]]]] with CacheAlways.Ops // scalastyle:ignore
    }
  }
}
