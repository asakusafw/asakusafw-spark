package com.asakusafw.spark.runtime.driver

import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.broadcast.Broadcast

import org.apache.spark.backdoor._
import org.apache.spark.util.backdoor.CallSite
import com.asakusafw.runtime.model.DataModel

abstract class MapDriver[T, B](
  sc: SparkContext,
  hadoopConf: Broadcast[Configuration],
  broadcasts: Map[B, Broadcast[_]],
  @transient prevs: Seq[RDD[(ShuffleKey, T)]])
    extends SubPlanDriver[B](sc, hadoopConf, broadcasts) with Branch[B, T] {
  assert(prevs.size > 0)

  override def execute(): Map[B, RDD[(ShuffleKey, _)]] = {
    sc.clearCallSite()
    sc.setCallSite(name)

    val prev = if (prevs.size == 1) prevs.head else new UnionRDD(sc, prevs)

    sc.setCallSite(CallSite(name, prev.toDebugString))
    branch(prev)
  }
}
