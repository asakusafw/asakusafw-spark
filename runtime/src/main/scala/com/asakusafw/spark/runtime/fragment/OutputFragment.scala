package com.asakusafw.spark.runtime.fragment

import scala.collection.mutable

import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.driver.PrepareKey

trait OutputFragment[B, T <: DataModel[T], K, V <: DataModel[V]] extends Fragment[T] {

  def branch: B

  def prepareKey: PrepareKey[B]

  def buffer(): Iterable[((B, K), V)]

  def flush(): Iterable[((B, K), V)]
}

abstract class OneToOneOutputFragment[B, T <: DataModel[T], K](
  val branch: B,
  val prepareKey: PrepareKey[B])
    extends OutputFragment[B, T, K, T] {

  def newDataModel(): T

  val buffer: mutable.ArrayBuffer[((B, K), T)] = mutable.ArrayBuffer.empty[((B, K), T)]

  override def add(result: T): Unit = {
    val dataModel = newDataModel()
    dataModel.copyFrom(result)
    buffer += (((branch, prepareKey.shuffleKey[K](branch, dataModel)), dataModel))
  }

  override def reset(): Unit = {
    buffer.clear()
  }

  override def flush(): Iterable[((B, K), T)] = {
    Iterable.empty
  }
}

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
