package com.asakusafw.spark.runtime.driver

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.rdd._

abstract class AggregateDriver[K: ClassTag, V: ClassTag, C <: DataModel[C], B](
  @transient val sc: SparkContext,
  @transient prev: RDD[(K, V)],
  @transient part: Partitioner)
    extends SubPlanDriver[B] with Branch[B] {

  override def execute(): Map[B, RDD[(_, _)]] = {
    val aggregate = if (prev.partitioner == Some(part)) {
      prev.asInstanceOf[RDD[(K, C)]].mapPartitions({ iter =>
        val combiner = aggregation.combinerCombiner
        combiner.insertAll(iter)
        val context = TaskContext.get
        new InterruptibleIterator(context, combiner.iterator)
      }, preservesPartitioning = true)
    } else {
      prev.combineByKey(
        aggregation.createCombiner _,
        aggregation.mergeValue _,
        aggregation.mergeCombiners _,
        part)
    }
    aggregate.branch[B, Any, Any](branchKeys, { iter =>
      val (fragment, outputs) = fragments
      assert(outputs.keys.toSet == branchKeys)

      def cast[K, V <: DataModel[V]](iterable: Iterable[((B, K), V)]) = {
        iterable.asInstanceOf[Iterable[((B, K), V)]]
      }

      iter.flatMap {
        case (_, dm) =>
          fragment.reset()
          fragment.add(dm)
          outputs.values.iterator.flatMap(output => cast(output.buffer))
      } ++ outputs.values.iterator.flatMap(output => cast(output.flush))
    },
      partitioners = partitioners,
      keyOrderings = orderings,
      preservesPartitioning = true)
  }

  def aggregation: Aggregation[K, V, C]

  def fragments[U <: DataModel[U]]: (Fragment[C], Map[B, OutputFragment[B, _, _, U]])
}
