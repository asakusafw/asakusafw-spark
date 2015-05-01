package com.asakusafw.spark.runtime.driver

import scala.concurrent.Future

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.rdd.BranchKey

abstract class SubPlanDriver(
    @transient val sc: SparkContext,
    val hadoopConf: Broadcast[Configuration],
    @transient val broadcasts: Map[BroadcastId, Future[Broadcast[_]]]) extends Serializable {

  def label: String

  def execute(): Map[BranchKey, Future[RDD[(ShuffleKey, _)]]]
}
