/*
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.spark.runtime.aggregation

import org.apache.spark.{ Aggregator, SparkEnv, TaskContext }
import org.apache.spark.util.collection.{ AppendOnlyMap, ExternalAppendOnlyMap }

import org.apache.spark.executor.backdoor._

abstract class Aggregation[K, V, C] extends Serializable {

  import Aggregation._ // scalastyle:ignore

  def newCombiner(): C

  def initCombinerByValue(combiner: C, value: V): C

  def mergeValue(combiner: C, value: V): C

  def initCombinerByCombiner(comb1: C, comb2: C): C

  def mergeCombiners(comb1: C, comb2: C): C

  lazy val isSpillEnabled =
    Option(SparkEnv.get).map(_.conf.getBoolean("spark.shuffle.spill", true)).getOrElse(false)

  def valueCombiner(): Combiner[K, V, C] = {
    if (!isSpillEnabled) {
      new InMemoryCombiner(
        initCombinerByValue(newCombiner(), _), mergeValue _, mergeCombiners _)
    } else {
      new ExternalCombiner(
        initCombinerByValue(newCombiner(), _), mergeValue _, mergeCombiners _)
    }
  }

  def combinerCombiner(): Combiner[K, C, C] = {
    if (!isSpillEnabled) {
      new InMemoryCombiner(
        initCombinerByCombiner(newCombiner(), _), mergeCombiners _, mergeCombiners _)
    } else {
      new ExternalCombiner(
        initCombinerByCombiner(newCombiner(), _), mergeCombiners _, mergeCombiners _)
    }
  }
}

object Aggregation {

  trait Combiner[K, V, C] extends Iterable[(K, C)] {
    def insert(key: K, value: V): Unit
    def insertAll(iter: Iterator[_ <: Product2[K, V]]): Unit = {
      while (iter.hasNext) {
        val pair = iter.next()
        insert(pair._1, pair._2)
      }
    }
  }

  private class InMemoryCombiner[K, V, C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C) extends Combiner[K, V, C] {

    val combiners = new AppendOnlyMap[K, C]
    var kv: Product2[K, V] = null // scalastyle:ignore
    val update = (hadValue: Boolean, oldValue: C) => {
      if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
    }

    def insert(key: K, value: V): Unit = {
      kv = (key, value)
      combiners.changeValue(kv._1, update)
    }

    def iterator: Iterator[(K, C)] = combiners.iterator
  }

  private class ExternalCombiner[K, V, C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C) extends Combiner[K, V, C] {

    val combiners = new ExternalAppendOnlyMap[K, V, C](createCombiner, mergeValue, mergeCombiners)

    def insert(key: K, value: V): Unit = {
      combiners.insert(key, value)
    }

    def iterator: Iterator[(K, C)] = {
      Option(TaskContext.get).foreach { c =>
        c.taskMetrics.incMemoryBytesSpilled(combiners.memoryBytesSpilled)
        c.taskMetrics.incDiskBytesSpilled(combiners.diskBytesSpilled)
      }
      combiners.iterator
    }
  }
}
