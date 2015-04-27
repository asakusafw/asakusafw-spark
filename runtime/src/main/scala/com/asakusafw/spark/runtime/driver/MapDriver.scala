package com.asakusafw.spark.runtime.driver

import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.broadcast.Broadcast

import org.apache.spark.backdoor._
import org.apache.spark.util.backdoor.CallSite
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.rdd.BranchKey

abstract class MapDriver[T](
  sc: SparkContext,
  hadoopConf: Broadcast[Configuration],
  broadcasts: Map[BroadcastId, Broadcast[_]],
  @transient prevs: Seq[RDD[(_, T)]])
    extends SubPlanDriver(sc, hadoopConf, broadcasts) with Branching[T] {
  assert(prevs.size > 0,
    s"Previous RDDs should be more than 0: ${prevs.size}")

  override def execute(): Map[BranchKey, RDD[(ShuffleKey, _)]] = {
    sc.clearCallSite()
    sc.setCallSite(name)

    val prev = if (prevs.size == 1) {
      prevs.head
    } else {
      new UnionRDD(sc, prevs).coalesce(sc.defaultParallelism, shuffle = false)
    }

//    sc.setCallSite(CallSite(name, prev.toDebugString))
    branch(prev)
  }
}
