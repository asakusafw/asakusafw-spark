package com.asakusafw.spark.runtime.driver

import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd._

import org.apache.spark.backdoor._
import org.apache.spark.util.backdoor.CallSite
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.rdd._

abstract class AggregateDriver[V, C, B](
  sc: SparkContext,
  hadoopConf: Broadcast[Configuration],
  broadcasts: Map[B, Broadcast[_]],
  @transient prevs: Seq[RDD[(ShuffleKey, V)]],
  @transient directions: Seq[Boolean],
  @transient partitioner: Partitioner)
    extends SubPlanDriver[B](sc, hadoopConf, broadcasts) with Branch[B, C] {
  assert(prevs.size > 0,
    s"Previous RDDs should be more than 0: ${prevs.size}")

  override def execute(): Map[B, RDD[(ShuffleKey, _)]] = {
    val agg = aggregation
    val part = Some(partitioner)

    sc.clearCallSite()
    sc.setCallSite(name)

    val aggregated = {
      val ordering = Option(new ShuffleKey.SortOrdering(directions))
      if (agg.mapSideCombine) {
        confluent(
          prevs.map {
            case prev if prev.partitioner == part =>
              prev.asInstanceOf[RDD[(ShuffleKey, C)]]
            case prev =>
              prev.mapPartitions({ iter =>
                val combiner = agg.valueCombiner
                combiner.insertAll(iter)
                val context = TaskContext.get
                new InterruptibleIterator(context, combiner.iterator)
              }, preservesPartitioning = true)
          }, partitioner, ordering)
          .mapPartitions({ iter =>
            val combiner = agg.combinerCombiner
            combiner.insertAll(iter.map { case (k, v) => (k.dropOrdering, v) })
            val context = TaskContext.get
            new InterruptibleIterator(context, combiner.iterator)
          }, preservesPartitioning = true)
      } else {
        confluent(prevs, partitioner, ordering)
          .mapPartitions({ iter =>
            val combiner = agg.valueCombiner
            combiner.insertAll(iter.map { case (k, v) => (k.dropOrdering, v) })
            val context = TaskContext.get
            new InterruptibleIterator(context, combiner.iterator)
          }, preservesPartitioning = true)
      }
    }

    sc.setCallSite(CallSite(name, aggregated.toDebugString))
    branch(aggregated.asInstanceOf[RDD[(ShuffleKey, C)]])
  }

  def aggregation: Aggregation[ShuffleKey, V, C]
}
