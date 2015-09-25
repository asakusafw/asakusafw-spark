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
package com.asakusafw.yass
package flow

import scala.collection.mutable
import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.runtime.rdd.BranchKey

trait Source extends Node {

  def compute(rc: RoundContext): Map[BranchKey, Future[RDD[_]]]

  final def getOrCompute(rc: RoundContext): Map[BranchKey, Future[RDD[_]]] = {
    Source.getOrCompute(this)(rc)
  }

  def map[T, U: ClassTag](branchKey: BranchKey)(f: T => U): Source = {
    new MapPartitions(this, branchKey, {
      RoundContext => (Int, iter: Iterator[T]) => iter.map(f)
    }, preservesPartitioning = false)
  }

  def mapWithRoundContext[T, U: ClassTag](branchKey: BranchKey)(
    f: RoundContext => T => U): Source = {
    new MapPartitions(this, branchKey, {
      rc: RoundContext => (Int, iter: Iterator[T]) => iter.map(f(rc))
    }, preservesPartitioning = false)
  }

  def flatMap[T, U: ClassTag](branchKey: BranchKey)(f: T => TraversableOnce[U]): Source = {
    new MapPartitions(this, branchKey, {
      RoundContext => (Int, iter: Iterator[T]) => iter.flatMap(f)
    }, preservesPartitioning = false)
  }

  def flatMapWithRoundContext[T, U: ClassTag](branchKey: BranchKey)(
    f: RoundContext => T => TraversableOnce[U]): Source = {
    new MapPartitions(this, branchKey, {
      rc: RoundContext => (Int, iter: Iterator[T]) => iter.flatMap(f(rc))
    }, preservesPartitioning = false)
  }

  def mapPartitions[T, U: ClassTag](branchKey: BranchKey)(
    f: Iterator[T] => Iterator[U],
    preservesPartitioning: Boolean = false): Source = {
    new MapPartitions(this, branchKey, {
      RoundContext => (index: Int, iter: Iterator[T]) => f(iter)
    }, preservesPartitioning)
  }

  def mapPartitionsWithRoundContext[T, U: ClassTag](branchKey: BranchKey)(
    f: RoundContext => Iterator[T] => Iterator[U],
    preservesPartitioning: Boolean = false): Source = {
    new MapPartitions(this, branchKey, {
      rc: RoundContext => (Int, iter: Iterator[T]) => f(rc)(iter)
    }, preservesPartitioning)
  }

  def mapPartitionsWithIndex[T, U: ClassTag](branchKey: BranchKey)(
    f: (Int, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean = false): Source = {
    new MapPartitions(this, branchKey, {
      RoundContext => (index: Int, iter: Iterator[T]) => f(index, iter)
    }, preservesPartitioning)
  }

  def mapPartitionsWithIndexAndRoundContext[T, U: ClassTag](branchKey: BranchKey)(
    f: RoundContext => (Int, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean = false): Source = {
    new MapPartitions(this, branchKey, {
      rc: RoundContext => (index: Int, iter: Iterator[T]) => f(rc)(index, iter)
    }, preservesPartitioning)
  }
}

object Source {

  private[this] val generatedRDDs =
    mutable.WeakHashMap.empty[Source, mutable.Map[RoundContext, Map[BranchKey, Future[RDD[_]]]]]

  private def getOrCompute(source: Source)(rc: RoundContext): Map[BranchKey, Future[RDD[_]]] =
    synchronized {
      generatedRDDs
        .getOrElseUpdate(source, mutable.WeakHashMap.empty)
        .getOrElseUpdate(rc, source.compute(rc))
    }
}
