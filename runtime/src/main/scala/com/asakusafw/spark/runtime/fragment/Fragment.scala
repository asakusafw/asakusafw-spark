package com.asakusafw.spark.runtime.fragment

import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel

abstract class Fragment[T] extends Result[T] {

  def reset(): Unit
}
