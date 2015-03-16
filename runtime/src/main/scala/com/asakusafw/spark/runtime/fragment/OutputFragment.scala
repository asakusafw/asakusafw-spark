package com.asakusafw.spark.runtime.fragment

import scala.collection.mutable

import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.driver.PrepareKey

trait OutputFragment[B, V <: DataModel[V], K] extends Fragment[V] {

  def branch: B

  def prepareKey: PrepareKey[B]

  def newDataModel(): V

  def buffer(): Iterable[((B, K), V)]

  def flush(): Iterable[((B, K), V)]
}

abstract class OneToOneOutputFragment[B, V <: DataModel[V], K](
  val branch: B,
  val prepareKey: PrepareKey[B])
    extends OutputFragment[B, V, K] {

  val buffer: mutable.ArrayBuffer[((B, K), V)] = mutable.ArrayBuffer.empty[((B, K), V)]

  override def add(result: V): Unit = {
    val dataModel = newDataModel()
    dataModel.copyFrom(result)
    buffer += (((branch, prepareKey.shuffleKey[K](branch, dataModel)), dataModel))
  }

  override def reset(): Unit = {
    buffer.clear()
  }

  override def flush(): Iterable[((B, K), V)] = {
    Iterable.empty
  }
}
