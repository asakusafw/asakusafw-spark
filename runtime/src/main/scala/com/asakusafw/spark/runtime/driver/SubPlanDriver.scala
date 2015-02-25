package com.asakusafw.spark.runtime.driver

import org.apache.hadoop.io.Writable
import org.apache.spark._
import org.apache.spark.rdd.RDD

import com.asakusafw.runtime.model.DataModel

trait SubPlanDriver[B] extends Serializable {

  def sc: SparkContext

  def branchKeys: Set[B] = Set.empty

  def partitioners: Map[B, Partitioner] = Map.empty

  def orderings[K]: Map[B, Ordering[K]] = Map.empty

  def shuffleKey[T <: DataModel[T], U <: DataModel[U]](branch: B, value: T): U

  def execute(): Map[B, RDD[(_, _)]]
}
