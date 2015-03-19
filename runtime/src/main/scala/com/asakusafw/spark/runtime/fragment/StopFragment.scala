package com.asakusafw.spark.runtime.fragment

import com.asakusafw.runtime.model.DataModel

class StopFragment[T] extends Fragment[T] {

  override def add(result: T): Unit = {
  }

  override def reset(): Unit = {
  }
}

object StopFragment extends StopFragment[Nothing]
