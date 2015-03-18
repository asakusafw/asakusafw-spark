package com.asakusafw.spark.runtime.driver

import scala.collection.mutable

import org.apache.spark.Partitioner
import org.apache.spark.rdd._

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.rdd._

trait Branch[B, T] {

  def branchKeys: Set[B]

  def partitioners: Map[B, Partitioner]

  def orderings[K]: Map[B, Ordering[K]]

  def shuffleKey[U](branch: B, value: DataModel[_]): U

  def fragments[U <: DataModel[U]]: (Fragment[T], Map[B, OutputFragment[U]])

  def branch(rdd: RDD[(_, T)]): Map[B, RDD[(_, _)]] = {
    val f: (Iterator[(_, T)] => Iterator[((B, _), _)]) = { iter =>
      val (fragment, outputs) = fragments
      assert(outputs.keys.toSet == branchKeys)

      iter.flatMap {
        case (_, value) =>
          fragment.reset()
          fragment.add(value)
          outputs.iterator.flatMap {
            case (key, output) =>
              def prepare[V <: DataModel[V]](buffer: mutable.ArrayBuffer[_]) = {
                buffer.asInstanceOf[mutable.ArrayBuffer[V]]
                  .map(out => ((key, shuffleKey(key, out)), out))
              }
              prepare(output.buffer)
          }
      }
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
