package com.asakusafw.spark.runtime.fragment

import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel

abstract class EdgeFragment[T <: DataModel[T]](children: Seq[Fragment[T]]) extends Fragment[T] {
  assert(children.size > 1,
    s"The size of children should be greater than 1: ${children.size}")

  def newDataModel(): T

  private[this] val dataModel: T = newDataModel()

  override def add(result: T): Unit = {
    children.tail.foreach {
      case output =>
        dataModel.reset()
        dataModel.copyFrom(result)
        output.add(dataModel)
    }
    children.head.add(result)
  }

  override def reset(): Unit = {
    children.foreach(_.reset())
  }
}
