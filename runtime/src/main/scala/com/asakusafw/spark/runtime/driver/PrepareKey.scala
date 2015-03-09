package com.asakusafw.spark.runtime.driver

import com.asakusafw.runtime.model.DataModel

trait PrepareKey[B] {

  def shuffleKey[U](branch: B, value: DataModel[_]): U
}
