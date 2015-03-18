package com.asakusafw.spark.runtime.driver

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.rdd.RDD

import com.asakusafw.runtime.model.DataModel

abstract class MapDriver[T <: DataModel[T]: ClassTag, B](
  @transient val sc: SparkContext,
  @transient prevs: Seq[RDD[(_, T)]])
    extends SubPlanDriver[B] with Branch[B, T] {
  assert(prevs.size > 0)

  override def execute(): Map[B, RDD[(_, _)]] = {
    val union = (prevs.head /: prevs.tail) {
      case (union, rdd) => union.union(rdd)
    }
    branch(union)
  }
}
