/*
 * Copyright 2011-2015 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.spark.runtime

import scala.concurrent.{ ExecutionContext, Future }

import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.runtime.rdd._

abstract class SparkClient {

  def execute(conf: SparkConf): Int = {
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", kryoRegistrator)
    conf.set("spark.kryo.referenceTracking", false.toString)

    val sc = new SparkContext(conf)
    try {
      val hadoopConf = sc.broadcast(sc.hadoopConfiguration)
      execute(sc, hadoopConf)(SparkClient.executionContext)
    } finally {
      sc.stop()
    }
  }

  def execute(
    sc: SparkContext,
    hadoopConf: Broadcast[Configuration])(
      implicit ec: ExecutionContext): Int

  def kryoRegistrator: String

  def broadcastAsHash[V](
    sc: SparkContext,
    label: String,
    prev: Future[RDD[(ShuffleKey, V)]],
    sort: Option[Ordering[ShuffleKey]],
    grouping: Ordering[ShuffleKey],
    part: Partitioner)(
      implicit ec: ExecutionContext): Future[Broadcast[Map[ShuffleKey, Seq[V]]]] = {

    prev.map { p =>
      sc.clearCallSite()
      sc.setCallSite(label)

      val results = smcogroup(
        Seq((p.asInstanceOf[RDD[(ShuffleKey, _)]], sort)),
        part,
        grouping)
        .map { case (k, vs) => (k.dropOrdering, vs(0).toVector.asInstanceOf[Seq[V]]) }
        .collect()
        .toMap

      sc.broadcast(results)
    }
  }
}

object SparkClient {

  implicit lazy val executionContext: ExecutionContext = ExecutionContext.fromExecutor(null) // scalastyle:ignore
}
