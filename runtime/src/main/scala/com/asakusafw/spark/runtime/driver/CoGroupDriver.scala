package com.asakusafw.spark.runtime.driver

import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd._

import org.apache.spark.backdoor._
import org.apache.spark.util.backdoor._
import com.asakusafw.spark.runtime.rdd._

abstract class CoGroupDriver[B](
  sc: SparkContext,
  hadoopConf: Broadcast[Configuration],
  broadcasts: Map[B, Broadcast[_]],
  @transient prevs: Seq[(Seq[RDD[(ShuffleKey, _)]], Seq[Boolean])],
  @transient part: Partitioner)
    extends SubPlanDriver[B](sc, hadoopConf, broadcasts) with Branch[Seq[Iterable[_]]] {
  assert(prevs.size > 0,
    s"Previous RDDs should be more than 0: ${prevs.size}")

  override def execute(): Map[BranchKey, RDD[(ShuffleKey, _)]] = {
    sc.clearCallSite()
    sc.setCallSite(name)

    val cogrouped =
      smcogroup[ShuffleKey](
        prevs.map {
          case (rdds, directions) =>
            val ordering = Option(new ShuffleKey.SortOrdering(directions))
            (confluent[ShuffleKey, Any](rdds, part, ordering), ordering)
        },
        part,
        ShuffleKey.GroupingOrdering)
        .mapValues(_.toSeq).asInstanceOf[RDD[(ShuffleKey, Seq[Iterable[_]])]]

    sc.setCallSite(CallSite(name, cogrouped.toDebugString))
    branch(cogrouped)
  }
}
