package com.asakusafw.spark.runtime.fragment

abstract class CoGroupFragment {

  def add(groups: Seq[Iterable[_]]): Unit

  def reset(): Unit
}
