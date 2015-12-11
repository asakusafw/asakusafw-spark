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
package com.asakusafw.spark.extensions.iterativebatch.runtime
package iterative

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.iterative.launch.IterativeStageInfo
import com.asakusafw.spark.runtime.{ Props, RoundContext, SparkClient }

import com.asakusafw.spark.extensions.iterativebatch.runtime.iterative.IterativeBatchSparkClient._

abstract class IterativeBatchSparkClient extends SparkClient {

  override def execute(conf: SparkConf, stageInfo: IterativeStageInfo): Int = {
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", kryoRegistrator)
    conf.set("spark.kryo.referenceTracking", false.toString)

    implicit val sc = new SparkContext(conf)
    try {
      val numSlots = conf.getInt(Props.NumSlots, Props.DefaultNumSlots)
      val executor = newIterativeBatchExecutor(numSlots)

      execute(executor, stageInfo)
    } finally {
      sc.stop()
    }
  }

  def execute(
    executor: IterativeBatchExecutor,
    stageInfo: IterativeStageInfo)(
      implicit sc: SparkContext): Int = {
    executor.start()
    try {
      val cursor = stageInfo.newCursor()
      while (cursor.next()) {
        val conf = new Configuration(sc.hadoopConfiguration)
        conf.set(StageInfo.KEY_NAME, cursor.get.serialize)

        executor.submit(Context(sc.broadcast(conf)))
      }
      executor.stop(awaitExecution = true, gracefully = true)
      0
    } catch {
      case NonFatal(t) =>
        executor.stop()
        throw t
    }
  }

  def newIterativeBatchExecutor(
    numSlots: Int)(
      implicit ec: ExecutionContext, sc: SparkContext): IterativeBatchExecutor

  def kryoRegistrator: String
}

object IterativeBatchSparkClient {

  case class Context(
    hadoopConf: Broadcasted[Configuration])
    extends RoundContext

  implicit lazy val ec: ExecutionContext = ExecutionContext.fromExecutor(null) // scalastyle:ignore
}