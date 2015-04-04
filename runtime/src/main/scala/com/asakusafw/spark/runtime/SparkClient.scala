package com.asakusafw.spark.runtime

import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.runtime.rdd._

abstract class SparkClient {

  def execute(conf: SparkConf): Int = {
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", kryoRegistrator)

    val sc = new SparkContext(conf)
    try {
      val hadoopConf = sc.broadcast(sc.hadoopConfiguration)
      execute(sc, hadoopConf)
    } finally {
      sc.stop()
    }
  }

  def execute(sc: SparkContext, hadoopConf: Broadcast[Configuration]): Int

  def kryoRegistrator: String

  def broadcastAsHash[V](
    sc: SparkContext,
    rdds: Seq[RDD[(ShuffleKey, V)]],
    directions: Seq[Boolean],
    part: Partitioner): Broadcast[Map[ShuffleKey, Seq[V]]] = {

    val ordering = Option(new ShuffleKey.SortOrdering(directions))
    sc.broadcast(
      smcogroup(
        Seq((confluent(rdds, part, ordering).asInstanceOf[RDD[(ShuffleKey, _)]], ordering)),
        part,
        ShuffleKey.GroupingOrdering)
        .map { case (k, vs) => (k.dropOrdering, vs(0).toSeq.asInstanceOf[Seq[V]]) }
        .collect.toMap.withDefault(_ => Seq.empty))
  }
}
