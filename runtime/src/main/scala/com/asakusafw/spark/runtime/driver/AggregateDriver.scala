package com.asakusafw.spark.runtime.driver

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.rdd._

abstract class AggregateDriver[K: ClassTag, V: ClassTag, C <: DataModel[C], B](
  @transient val sc: SparkContext,
  @transient prevs: Seq[RDD[(K, V)]],
  @transient partitioner: Partitioner)
    extends SubPlanDriver[B] with Branch[B, C] {
  assert(prevs.size > 0)

  override def execute(): Map[B, RDD[(_, _)]] = {
    val agg = aggregation
    val part = Some(partitioner)

    sc.setCallSite(name)
    val aggregated =
      if (agg.mapSideCombine && prevs.exists(_.partitioner == part)) {
        confluent(
          prevs.map {
            case prev if prev.partitioner == part =>
              prev.asInstanceOf[RDD[(K, C)]]
            case prev =>
              prev.mapPartitions({ iter =>
                val combiner = agg.valueCombiner
                combiner.insertAll(iter)
                val context = TaskContext.get
                new InterruptibleIterator(context, combiner.iterator)
              }, preservesPartitioning = true).shuffle(partitioner, None)
          }, partitioner, None)
          .mapPartitions({ iter =>
            val combiner = agg.combinerCombiner
            combiner.insertAll(iter)
            val context = TaskContext.get
            new InterruptibleIterator(context, combiner.iterator)
          }, preservesPartitioning = true)
      } else {
        new ShuffledRDD(
          new UnionRDD(sc, prevs),
          partitioner)
          .setAggregator(agg.aggregator)
          .setMapSideCombine(agg.mapSideCombine)
      }
    branch(aggregated.asInstanceOf[RDD[(_, C)]])
  }

  def aggregation: Aggregation[K, V, C]
}
