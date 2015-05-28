package com.asakusafw.spark.runtime.fragment

import scala.reflect.ClassTag

import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel

import org.apache.spark.util.collection.backdoor.CompactBuffer

abstract class OutputFragment[T <: DataModel[T]: ClassTag] extends Fragment[T] with Iterable[T] {

  def newDataModel(): T

  private[this] val buf = new CompactBuffer[T]
  private[this] var curSize = 0

  override def iterator: Iterator[T] = buf.iterator.take(curSize)

  override def add(result: T): Unit = {
    if (buf.size <= curSize) {
      buf += newDataModel()
    }
    buf(curSize).copyFrom(result)
    curSize += 1
  }

  override def reset(): Unit = {
    curSize = 0
  }
}
