package com.asakusafw.spark.runtime

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

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

  def broadcastAsHash[K: ClassTag, V](
    sc: SparkContext,
    rdds: Seq[RDD[(K, V)]],
    part: Partitioner,
    ordering: Ordering[K],
    grouping: Ordering[K]): Broadcast[Map[K, Seq[V]]] = {

    sc.broadcast(
      smcogroup[K](
        Seq((confluent(rdds, part, Option(ordering)).asInstanceOf[RDD[(K, _)]], Option(ordering))),
        part,
        grouping)
        .mapValues(_.head.toSeq.asInstanceOf[Seq[V]]).collect.toMap.withDefault(_ => Seq.empty))
  }
}
