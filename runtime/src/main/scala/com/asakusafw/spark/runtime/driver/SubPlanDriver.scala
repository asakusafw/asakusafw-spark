package com.asakusafw.spark.runtime.driver

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.runtime.model.DataModel

abstract class SubPlanDriver[B](
    @transient val sc: SparkContext,
    val hadoopConf: Broadcast[Configuration],
    val broadcasts: Map[B, Broadcast[_]]) extends Serializable {

  def name: String

  def execute(): Map[B, RDD[(ShuffleKey, _)]]
}
