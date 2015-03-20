package com.asakusafw.spark.runtime.driver

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import com.asakusafw.spark.runtime.rdd._

abstract class CoGroupDriver[B, K: ClassTag](
  @transient val sc: SparkContext,
  @transient inputs: Seq[(Seq[RDD[(K, _)]], Option[Ordering[K]])],
  @transient part: Partitioner,
  @transient grouping: Ordering[K])
    extends SubPlanDriver[B] with Branch[B, Seq[Iterable[_]]] {
  assert(inputs.size > 0)

  override def execute(): Map[B, RDD[(_, _)]] = {
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
    branch(cogrouped)
  }
}
