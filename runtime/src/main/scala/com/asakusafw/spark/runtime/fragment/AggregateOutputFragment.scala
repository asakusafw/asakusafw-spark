package com.asakusafw.spark.runtime.fragment

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.driver.PrepareKey

class AggregateOutputFragment[B, T <: DataModel[T], K, V <: DataModel[V]](
  val aggregation: Aggregation[(B, K), T, V],
  val branch: B,
  val prepareKey: PrepareKey[B])
    extends OutputFragment[B, T, K, V] {

  val valueCombiner = aggregation.valueCombiner()

  override def add(result: T): Unit = {
    valueCombiner.insert((branch, prepareKey.shuffleKey[K](branch, result)), result)
  }

  override def reset(): Unit = {
  }

  override def buffer: Iterable[((B, K), V)] = {
    Iterable.empty
  }

  override def flush(): Iterable[((B, K), V)] = {
    valueCombiner
  }
}
