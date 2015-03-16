package com.asakusafw.spark.runtime.driver

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.rdd._

abstract class CoGroupDriver[B, K](
  @transient val sc: SparkContext,
  @transient inputs: Seq[(RDD[(K, _)], Option[Ordering[K]])],
  @transient part: Partitioner,
  @transient grouping: Ordering[K])
    extends SubPlanDriver[B] with Branch[B] {
  assert(inputs.size > 0)

  override def execute(): Map[B, RDD[(_, _)]] = {
    val cogroup = smcogroup[K](inputs, part, grouping)
    cogroup.branch[B, Any, Any](branchKeys, { iter =>
      val (fragment, outputs) = fragments
      assert(outputs.keys.toSet == branchKeys)

      def cast[K, V <: DataModel[V]](iterable: Iterable[((B, K), V)]) = {
        iterable.asInstanceOf[Iterable[((B, K), V)]]
      }

      iter.flatMap {
        case (_, iterables) =>
          fragment.reset()
          fragment.add(iterables)
          outputs.values.iterator.flatMap(output => cast(output.buffer))
      } ++ outputs.values.iterator.flatMap(output => cast(output.flush))
    },
      partitioners = partitioners,
      keyOrderings = orderings,
      preservesPartitioning = true)
  }

  def fragments[U <: DataModel[U]]: (CoGroupFragment, Map[B, OutputFragment[B, _, _, U]])
}
