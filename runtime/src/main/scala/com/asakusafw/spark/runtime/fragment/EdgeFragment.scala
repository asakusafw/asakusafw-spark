package com.asakusafw.spark.runtime.fragment

import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel

abstract class EdgeFragment[T <: DataModel[T]](children: Array[Fragment[T]]) extends Fragment[T] {
  assert(children.size > 1,
    s"The size of children should be greater than 1: ${children.size}")

  def newDataModel(): T

  private[this] val dataModel: T = newDataModel()

  private[this] val size = children.length

  private[this] var reset = true

  override def add(result: T): Unit = {
    reset = false
    var i = 0
    while (i < size - 1) {
      dataModel.copyFrom(result)
      children(i).add(dataModel)
      i += 1
    }
    children(i).add(result)
  }

  override def reset(): Unit = {
    if (!reset) {
      var i = 0
      while (i < size) {
        children(i).reset()
        i += 1
      }
      reset = true
    }
  }
}
