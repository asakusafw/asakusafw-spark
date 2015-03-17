package com.asakusafw.spark.runtime.driver

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.fragment._

abstract class AggregateDriver[K: ClassTag, V: ClassTag, C <: DataModel[C], B](
  @transient val sc: SparkContext,
  @transient prev: RDD[(K, V)],
  @transient part: Partitioner)
    extends SubPlanDriver[B] with Branch[B, C] {

  override def execute(): Map[B, RDD[(_, _)]] = {
    val aggregated = if (prev.partitioner == Some(part)) {
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
    branch(aggregated.asInstanceOf[RDD[(_, C)]])
  }

  def aggregation: Aggregation[K, V, C]
}
