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
