package com.asakusafw.spark.runtime.driver

import java.io.{ DataInput, DataOutput }

import scala.annotation.tailrec

import org.apache.hadoop.io.Writable

import org.apache.spark.util.collection.backdoor.CompactBuffer
import com.asakusafw.runtime.value.ValueOption

class ShuffleKey(
    val grouping: Seq[ValueOption[_]],
    val ordering: Seq[ValueOption[_]]) extends Writable with Equals {

  override def write(out: DataOutput): Unit = {
    grouping.foreach(_.write(out))
    ordering.foreach(_.write(out))
  }

  override def readFields(in: DataInput): Unit = {
    grouping.foreach(_.readFields(in))
    ordering.foreach(_.readFields(in))
  }

  override def hashCode: Int = grouping.hashCode

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ShuffleKey =>
        (that canEqual this) && (this.grouping == that.grouping) && (this.ordering == that.ordering)
      case _ => false
    }
  }

  override def canEqual(obj: Any): Boolean = {
    obj.isInstanceOf[ShuffleKey]
  }

  def dropOrdering: ShuffleKey = new ShuffleKey(grouping, CompactBuffer())
}

object ShuffleKey {

  private[this] def compare0(xs: Seq[ValueOption[_]], ys: Seq[ValueOption[_]], ascs: Array[Boolean]): Int = {
    val size = xs.size
    var i = 0
    while (i < size) {
      val cmp = if (ascs(i)) {
        xs(i).compareTo(ys(i))
      } else {
        ys(i).compareTo(xs(i))
      }
      if (cmp != 0) {
        return cmp
      }
      i += 1
    }
    0
  }

  object GroupingOrdering extends Ordering[ShuffleKey] {

    override def compare(x: ShuffleKey, y: ShuffleKey): Int = {
      assert(x.grouping.size == y.grouping.size,
        s"The size of grouping keys should be the same: (${x.grouping.size}, ${y.grouping.size})")
      assert(x.grouping.zip(y.grouping).forall { case (x, y) => x.getClass == y.getClass },
        s"The all of types of grouping keys should be the same: (${
          x.grouping.map(_.getClass).mkString("(", ",", ")")
        }, ${
          y.grouping.map(_.getClass).mkString("(", ",", ")")
        })")

      compare0(x.grouping, y.grouping, Array.fill(x.grouping.size)(true))
    }
  }

  class SortOrdering(directions: Array[Boolean]) extends Ordering[ShuffleKey] {

    override def compare(x: ShuffleKey, y: ShuffleKey): Int = {
      assert(x.grouping.size == y.grouping.size,
        s"The size of grouping keys should be the same: (${x.grouping.size}, ${y.grouping.size})")
      assert(x.grouping.zip(y.grouping).forall { case (x, y) => x.getClass == y.getClass },
        s"The all of types of grouping keys should be the same: (${
          x.grouping.map(_.getClass).mkString("(", ",", ")")
        }, ${
          y.grouping.map(_.getClass).mkString("(", ",", ")")
        })")
      assert(x.ordering.size == y.ordering.size,
        s"The size of ordering keys should be the same: (${x.grouping.size}, ${y.grouping.size})")
      assert(x.ordering.zip(y.ordering).forall { case (x, y) => x.getClass == y.getClass },
        s"The all of types of ordering keys should be the same: (${
          x.grouping.map(_.getClass).mkString("(", ",", ")")
        }, ${
          y.grouping.map(_.getClass).mkString("(", ",", ")")
        })")
      assert(directions.length == x.ordering.size,
        s"The size of directions should be the same as ordering keys: (${directions.length}, ${x.ordering.size})")

      val cmp = GroupingOrdering.compare(x, y)
      if (cmp == 0) {
        compare0(x.ordering, y.ordering, directions)
      } else {
        cmp
      }
    }
  }
}
