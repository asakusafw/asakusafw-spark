package com.asakusafw.spark.runtime.driver

import java.io.{ DataInput, DataOutput }

import scala.annotation.tailrec

import com.asakusafw.runtime.value.ValueOption

class ShuffleKey(
    val grouping: Seq[ValueOption[_]],
    val ordering: Seq[ValueOption[_]]) extends Equals {

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

  def dropOrdering: ShuffleKey = new ShuffleKey(grouping, Seq.empty)
}

object ShuffleKey {

  @tailrec
  private[this] def compare0(xs: Seq[ValueOption[_]], ys: Seq[ValueOption[_]], ascs: Seq[Boolean]): Int = {
    if (xs.isEmpty) {
      0
    } else {
      val cmp = if (ascs.head) {
        xs.head.compareTo(ys.head)
      } else {
        ys.head.compareTo(xs.head)
      }
      if (cmp == 0) {
        compare0(xs.tail, ys.tail, ascs.tail)
      } else {
        cmp
      }
    }
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

      compare0(x.grouping, y.grouping, Seq.fill(x.grouping.size)(true))
    }
  }

  class SortOrdering(directions: Seq[Boolean]) extends Ordering[ShuffleKey] {

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
      assert(directions.size == x.ordering.size,
        s"The side of directions should be the same as ordering keys: (${directions.size}, ${x.ordering.size})")

      val cmp = GroupingOrdering.compare(x, y)
      if (cmp == 0) {
        compare0(x.ordering, y.ordering, directions)
      } else {
        cmp
      }
    }
  }
}
