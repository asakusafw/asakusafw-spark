package com.asakusafw.spark.runtime.orderings

class HashOrdering[T] extends Ordering[T] {

  override def compare(left: T, right: T): Int = {
    left.hashCode.compare(right.hashCode)
  }
}
