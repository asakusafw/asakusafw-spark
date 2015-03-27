package com.asakusafw.spark.runtime.driver

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd._

import org.apache.spark.backdoor._
import org.apache.spark.util.backdoor._
import com.asakusafw.spark.runtime.rdd._

abstract class CoGroupDriver[B, K: ClassTag](
  @transient val sc: SparkContext,
  val hadoopConf: Broadcast[Configuration],
  @transient inputs: Seq[(Seq[RDD[(K, _)]], Option[Ordering[K]])],
  @transient part: Partitioner,
  @transient grouping: Ordering[K])
    extends SubPlanDriver[B] with Branch[B, Seq[Iterable[_]]] {
  assert(inputs.size > 0)

  override def execute(): Map[B, RDD[(_, _)]] = {
    sc.clearCallSite()
    sc.setCallSite(name)

    val cogrouped =
      smcogroup[K](
        inputs.map {
          case (rdds, ordering) =>
            (confluent[K, Any](rdds, part, ordering), ordering)
        },
        part,
        grouping)
        .mapValues(_.toSeq).asInstanceOf[RDD[(_, Seq[Iterable[_]])]]

    sc.setCallSite(CallSite(name, cogrouped.toDebugString))
    branch(cogrouped)
  }
}
