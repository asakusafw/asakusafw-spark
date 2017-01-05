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

import scala.reflect.ClassTag

import com.asakusafw.spark.runtime.RoundContext
import com.asakusafw.spark.runtime.graph.Source
import com.asakusafw.spark.runtime.rdd.BranchKey

object RoundAwareSource {

  trait Ops extends Source.Ops {
    self: Source =>

    override def map[T, U: ClassTag](branchKey: BranchKey)(
      f: T => U): Source with Ops

    def mapWithRoundContext[T, U: ClassTag](branchKey: BranchKey)(
      f: RoundContext => T => U): Source with Ops

    override def flatMap[T, U: ClassTag](branchKey: BranchKey)(
      f: T => TraversableOnce[U]): Source with Ops

    def flatMapWithRoundContext[T, U: ClassTag](branchKey: BranchKey)(
      f: RoundContext => T => TraversableOnce[U]): Source with Ops

    override def mapPartitions[T, U: ClassTag](branchKey: BranchKey)(
      f: Iterator[T] => Iterator[U],
      preservesPartitioning: Boolean = false): Source with Ops

    def mapPartitionsWithRoundContext[T, U: ClassTag](branchKey: BranchKey)(
      f: RoundContext => Iterator[T] => Iterator[U],
      preservesPartitioning: Boolean = false): Source with Ops

    override def mapPartitionsWithIndex[T, U: ClassTag](branchKey: BranchKey)(
      f: (Int, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false): Source with Ops

    def mapPartitionsWithIndexAndRoundContext[T, U: ClassTag](branchKey: BranchKey)(
      f: RoundContext => (Int, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false): Source with Ops
  }
}
