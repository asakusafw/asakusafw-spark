package com.asakusafw.spark.runtime.fragment

import scala.collection.mutable

import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel

abstract class OutputFragment[T <: DataModel[T]] extends Fragment[T] {

  val buffer: mutable.ArrayBuffer[T] = mutable.ArrayBuffer.empty[T]

  def newDataModel(): T

  override def add(result: T): Unit = {
    val dataModel = newDataModel()
    dataModel.copyFrom(result)
    buffer += dataModel
  }

  override def reset(): Unit = {
    buffer.clear()
  }
}
