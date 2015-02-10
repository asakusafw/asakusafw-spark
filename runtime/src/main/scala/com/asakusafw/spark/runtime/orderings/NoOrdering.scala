package com.asakusafw.spark.runtime.orderings

class NoOrdering[T] extends Ordering[T] {

  override def compare(left: T, right: T): Int = 0
}
