package com.asakusafw.spark.runtime.fragment

import scala.collection.mutable

import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel

abstract class OutputFragment[T <: DataModel[T]] extends Fragment[T] with Iterable[T] {

  def newDataModel(): T

  private[this] val buf = mutable.ArrayBuffer.empty[T]

  override def iterator: Iterator[T] = buf.iterator

  override def add(result: T): Unit = {
    val dataModel = newDataModel()
    dataModel.copyFrom(result)
    buf += dataModel
  }

  override def reset(): Unit = {
    if (buf.nonEmpty) {
      buf.clear()
    }
  }
}
