package com.asakusafw.spark.runtime.driver

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.rdd._

import com.asakusafw.runtime.model.DataModel

abstract class MapDriver[T <: DataModel[T]: ClassTag, B](
  @transient val sc: SparkContext,
  @transient prevs: Seq[RDD[(_, T)]])
    extends SubPlanDriver[B] with Branch[B, T] {
  assert(prevs.size > 0)

  override def execute(): Map[B, RDD[(_, _)]] = {
    sc.setCallSite(name)
    branch(new UnionRDD(sc, prevs))
  }
}
