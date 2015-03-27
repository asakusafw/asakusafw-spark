package com.asakusafw.spark.runtime.driver

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.broadcast.Broadcast

import org.apache.spark.backdoor._
import org.apache.spark.util.backdoor.CallSite
import com.asakusafw.runtime.model.DataModel

abstract class MapDriver[T <: DataModel[T]: ClassTag, B](
  @transient val sc: SparkContext,
  val hadoopConf: Broadcast[Configuration],
  @transient prevs: Seq[RDD[(_, T)]])
    extends SubPlanDriver[B] with Branch[B, T] {
  assert(prevs.size > 0)

  override def execute(): Map[B, RDD[(_, _)]] = {
    sc.clearCallSite()
    sc.setCallSite(name)

    val prev = if (prevs.size == 1) prevs.head else new UnionRDD(sc, prevs)

    sc.setCallSite(CallSite(name, prev.toDebugString))
    branch(prev)
  }
}
