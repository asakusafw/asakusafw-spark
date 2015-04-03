package com.asakusafw.spark.runtime.aggregation

import org.apache.spark.{ Aggregator, SparkEnv, TaskContext }
import org.apache.spark.util.collection.{ AppendOnlyMap, ExternalAppendOnlyMap }

abstract class Aggregation[K, V, C] extends Serializable {

  import Aggregation._

  def mapSideCombine: Boolean

  def createCombiner(value: V): C

  def mergeValue(combiner: C, value: V): C

  def mergeCombiners(comb1: C, comb2: C): C

  lazy val isSpillEnabled =
    Option(SparkEnv.get).map(_.conf.getBoolean("spark.shuffle.spill", true)).getOrElse(false)

  def aggregator(): Aggregator[K, V, C] = Aggregator(createCombiner _, mergeValue _, mergeCombiners _)

  def valueCombiner(): Combiner[K, V, C] = {
    if (!isSpillEnabled) {
      new InMemoryCombiner(createCombiner _, mergeValue _, mergeCombiners _)
    } else {
      new ExternalCombiner(createCombiner _, mergeValue _, mergeCombiners _)
    }
  }

  def combinerCombiner(): Combiner[K, C, C] = {
    if (!isSpillEnabled) {
      new InMemoryCombiner(identity, mergeCombiners _, mergeCombiners _)
    } else {
      new ExternalCombiner(identity, mergeCombiners _, mergeCombiners _)
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
    var kv: Product2[K, V] = null
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
        c.taskMetrics.memoryBytesSpilled += combiners.memoryBytesSpilled
        c.taskMetrics.diskBytesSpilled += combiners.diskBytesSpilled
      }
      combiners.iterator
    }
  }
}
