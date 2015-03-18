package com.asakusafw.spark.runtime.driver

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.rdd.RDD

import com.asakusafw.runtime.model.DataModel

abstract class MapDriver[T <: DataModel[T]: ClassTag, B](
  @transient val sc: SparkContext,
  @transient prev: RDD[(_, T)])
    extends SubPlanDriver[B] with Branch[B, T] {

  override def execute(): Map[B, RDD[(_, _)]] = {
    branch(prev)
  }
}
