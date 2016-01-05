/*
 * Copyright 2011-2016 Asakusa Framework Team.
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

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }
import org.slf4j.LoggerFactory

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.iterative.launch.IterativeStageInfo
import com.asakusafw.spark.runtime.{ Props, RoundContext, SparkClient }
import com.asakusafw.spark.runtime.SparkClient.Implicits.ec

import com.asakusafw.spark.extensions.iterativebatch.runtime.iterative.IterativeBatchSparkClient._

abstract class IterativeBatchSparkClient extends SparkClient {

  override def execute(conf: SparkConf, stageInfo: IterativeStageInfo): Int = {
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", kryoRegistrator)
    conf.set("spark.kryo.referenceTracking", false.toString)

    implicit val sc = SparkContext.getOrCreate(conf)
    try {
      val numSlots = conf.getInt(Props.NumSlots, Props.DefaultNumSlots)
      val stopOnFail = conf.getBoolean(Props.StopOnFail, Props.DefaultStopOnFail)
      val executor = newIterativeBatchExecutor(numSlots, stopOnFail)

      executor.addListener(Logger)

      execute(executor, stageInfo, stopOnFail)
    } finally {
      sc.stop()
    }
  }

  def execute(
    executor: IterativeBatchExecutor,
    stageInfo: IterativeStageInfo,
    stopOnFail: Boolean)(
      implicit sc: SparkContext): Int = {
    executor.start()
    try {
      val contexts = stageInfo.iterator.toSeq.map { stageInfo =>
        val conf = new Configuration(sc.hadoopConfiguration)
        conf.set(StageInfo.KEY_NAME, stageInfo.serialize)
        Context(sc.broadcast(conf))
      }
      executor.submitAll(contexts)
      executor.stop(awaitExecution = true, gracefully = true)

      if (stopOnFail &&
        contexts.map(executor.result).exists { result =>
          result.isEmpty || result.get.isFailure
        }) {
        1
      } else {
        0
      }
    } catch {
      case NonFatal(t) =>
        executor.stop()
        throw t
    }
  }

  def newIterativeBatchExecutor(
    numSlots: Int, stopOnFail: Boolean)(
      implicit ec: ExecutionContext, sc: SparkContext): IterativeBatchExecutor

  def kryoRegistrator: String
}

object IterativeBatchSparkClient {

  case class Context(
    hadoopConf: Broadcasted[Configuration])
    extends RoundContext {

    private def stageInfo: StageInfo =
      StageInfo.deserialize(hadoopConf.value.get(StageInfo.KEY_NAME))

    override lazy val roundId: Option[String] = {
      Option(stageInfo.getStageId)
    }

    override lazy val toString: String = stageInfo.toString
  }

  object Logger extends IterativeBatchExecutor.Listener {

    val Logger = LoggerFactory.getLogger(getClass)

    override def onExecutorStart(): Unit = {
      if (Logger.isInfoEnabled) {
        Logger.info("IterativaBatchExecutor started.")
      }
    }

    override def onRoundSubmitted(rc: RoundContext): Unit = {
      if (Logger.isInfoEnabled) {
        Logger.info(s"Round[${rc}] is submitted.")
      }
    }

    override def onRoundStart(rc: RoundContext): Unit = {
      if (Logger.isInfoEnabled) {
        Logger.info(s"Round[${rc}] started.")
      }
    }

    override def onRoundCompleted(rc: RoundContext, result: Try[Unit]): Unit = {
      result match {
        case Success(_) =>
          if (Logger.isInfoEnabled) {
            Logger.info(s"Round[${rc}] successfully completed.")
          }
        case Failure(t) =>
          if (Logger.isErrorEnabled) {
            Logger.error(s"Round[${rc}] failed.", t)
          }
      }
    }

    override def onExecutorStop(): Unit = {
      if (Logger.isInfoEnabled) {
        Logger.info("IterativaBatchExecutor stopped.")
      }
    }
  }
}
