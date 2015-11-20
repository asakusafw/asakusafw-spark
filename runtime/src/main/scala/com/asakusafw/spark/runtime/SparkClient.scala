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

import scala.concurrent.{ Await, ExecutionContext }
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }

import com.asakusafw.spark.runtime.graph.Job

abstract class SparkClient {

  def execute(conf: SparkConf): Int = {
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", kryoRegistrator)
    conf.set("spark.kryo.referenceTracking", false.toString)

    val sc = new SparkContext(conf)
    try {
      val job = newJob(sc)
      val hadoopConf = sc.broadcast(sc.hadoopConfiguration)
      val context = SparkClient.Context(hadoopConf)
      Await.result(job.execute(context)(SparkClient.ec), Duration.Inf)
      0
    } finally {
      sc.stop()
    }
  }

  def newJob(sc: SparkContext): Job

  def kryoRegistrator: String
}

object SparkClient {

  case class Context(
    hadoopConf: Broadcasted[Configuration])
    extends RoundContext

  lazy val ec: ExecutionContext = ExecutionContext.fromExecutor(null) // scalastyle:ignore
}
