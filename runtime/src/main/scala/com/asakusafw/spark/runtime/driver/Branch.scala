package com.asakusafw.spark.runtime.driver

import org.apache.spark.Partitioner
import org.apache.spark.rdd._

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.rdd._

trait Branch[B, T] extends PrepareKey[B] {

  def branchKeys: Set[B]

  def partitioners: Map[B, Partitioner]

  def orderings[K]: Map[B, Ordering[K]]

  def fragments[U <: DataModel[U]]: (Fragment[T], Map[B, OutputFragment[B, _, _, U]])

  def branch(rdd: RDD[(_, T)]): Map[B, RDD[(_, _)]] = {
    val f: (Iterator[(_, T)] => Iterator[((B, _), _)]) = { iter =>
      val (fragment, outputs) = fragments
      assert(outputs.keys.toSet == branchKeys)

      def cast[K, V <: DataModel[V]](iterable: Iterable[((B, K), V)]) = {
        iterable.asInstanceOf[Iterable[((B, K), V)]]
      }

      iter.flatMap {
        case (_, value) =>
          fragment.reset()
          fragment.add(value)
          outputs.values.iterator.flatMap(output => cast(output.buffer))
      } ++ outputs.values.iterator.flatMap(output => cast(output.flush))
    }
    if (branchKeys.size == 1 && partitioners.size == 0) {
      Map(branchKeys.head -> rdd.mapPartitions({ iter =>
        f(iter).map {
          case ((_, k), v) => (k, v)
        }
      }, preservesPartitioning = true))
    } else {
      rdd.branch[B, Any, Any](
        branchKeys,
        f,
        partitioners = partitioners,
        keyOrderings = orderings,
        preservesPartitioning = true)
    }
  }
}
