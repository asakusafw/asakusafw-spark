package com.asakusafw.spark.runtime.driver

import java.util.Arrays

import com.asakusafw.runtime.io.util.WritableRawComparable

class ShuffleKey(
    var grouping: Array[Byte],
    var ordering: Array[Byte]) extends Equals {

  def this() = this(Array.empty, Array.empty)

  override def hashCode: Int = Arrays.hashCode(grouping)

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ShuffleKey =>
        (that canEqual this) &&
          Arrays.equals(this.grouping, that.grouping) &&
          Arrays.equals(this.ordering, that.ordering)
      case _ => false
    }
  }

  override def canEqual(obj: Any): Boolean = {
    obj.isInstanceOf[ShuffleKey]
  }

  def dropOrdering: ShuffleKey = new ShuffleKey(grouping, Array.empty)
}
