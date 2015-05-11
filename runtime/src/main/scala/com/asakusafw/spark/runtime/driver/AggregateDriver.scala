package com.asakusafw.spark.runtime.driver

import scala.concurrent.Future

import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd._

import org.apache.spark.backdoor._
import org.apache.spark.util.backdoor.CallSite
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.SparkClient.executionContext
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.rdd._

abstract class AggregateDriver[V, C](
  sc: SparkContext,
  hadoopConf: Broadcast[Configuration],
  broadcasts: Map[BroadcastId, Future[Broadcast[_]]],
  @transient prevs: Seq[Future[RDD[(ShuffleKey, V)]]],
  @transient sort: Option[ShuffleKey.SortOrdering],
  @transient partitioner: Partitioner)
    extends SubPlanDriver(sc, hadoopConf, broadcasts) with Branching[C] {
  assert(prevs.size > 0,
    s"Previous RDDs should be more than 0: ${prevs.size}")

  override def execute(): Map[BranchKey, Future[RDD[(ShuffleKey, _)]]] = {
    val agg = aggregation
    val part = Some(partitioner)

    val future = Future.sequence(prevs).map { prevs =>
      sc.clearCallSite()
      sc.setCallSite(label)

      val aggregated = {
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
            }, partitioner, sort)
            .mapPartitions({ iter =>
              val combiner = agg.combinerCombiner
              combiner.insertAll(iter.map { case (k, v) => (k.dropOrdering, v) })
              val context = TaskContext.get
              new InterruptibleIterator(context, combiner.iterator)
            }, preservesPartitioning = true)
        } else {
          confluent(prevs, partitioner, sort)
            .mapPartitions({ iter =>
              val combiner = agg.valueCombiner
              combiner.insertAll(iter.map { case (k, v) => (k.dropOrdering, v) })
              val context = TaskContext.get
              new InterruptibleIterator(context, combiner.iterator)
            }, preservesPartitioning = true)
        }
      }

      branch(aggregated.asInstanceOf[RDD[(_, C)]])
    }

    branchKeys.map(key => key -> future.map(_(key))).toMap
  }

  def aggregation: Aggregation[ShuffleKey, V, C]
}
