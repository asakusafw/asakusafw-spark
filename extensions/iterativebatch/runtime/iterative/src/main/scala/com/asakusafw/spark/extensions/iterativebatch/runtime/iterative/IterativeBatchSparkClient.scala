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
import com.asakusafw.spark.runtime
import com.asakusafw.spark.runtime.{ JobContext, Props, RoundContext, SparkClient }
import com.asakusafw.spark.runtime.SparkClient.Implicits.ec

import com.asakusafw.spark.extensions.iterativebatch.runtime.graph.IterativeJob

abstract class IterativeBatchSparkClient extends SparkClient {

  val Logger = LoggerFactory.getLogger(getClass)

  override def execute(conf: SparkConf, stageInfo: IterativeStageInfo): Int = {
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", kryoRegistrator)
    conf.set("spark.kryo.referenceTracking", false.toString)

    val sc = SparkContext.getOrCreate(conf)
    try {
      val numSlots = conf.getInt(Props.NumSlots, Props.DefaultNumSlots)
      val stopOnFail = conf.getBoolean(Props.StopOnFail, Props.DefaultStopOnFail)

      implicit val jobContext = IterativeBatchSparkClient.JobContext(sc)
      val job = newJob(jobContext)

      execute(job, stageInfo, numSlots, stopOnFail)
    } finally {
      sc.stop()
    }
  }

  def execute(
    job: IterativeJob,
    stageInfo: IterativeStageInfo,
    numSlots: Int,
    stopOnFail: Boolean)(
      implicit jobContext: JobContext): Int = {
    val executor = new IterativeBatchExecutor(numSlots, stopOnFail)(job)
    executor.addListener(IterativeBatchSparkClient.Logger)
    executor.start()
    try {
      val origin = newContext(stageInfo.getOrigin)
      val contexts = stageInfo.iterator.toSeq.map(newContext)
      executor.submitAll(contexts)
      executor.stop(awaitExecution = true, gracefully = true)

      if (stopOnFail &&
        contexts.map(executor.result).exists { result =>
          result.isEmpty || result.get.isFailure
        }) {
        1
      } else {
        val contextsToCommit = contexts.filter { context =>
          val result = executor.result(context)
          result.isDefined && result.get.isSuccess
        }
        job.commit(origin, contextsToCommit)

        if (Logger.isInfoEnabled) {
          import Logger._ // scalastyle:ignore
          info(s"Direct I/O file output: ${jobContext.outputStatistics.size} entries")
          jobContext.outputStatistics.toSeq.sortBy(_._1).foreach {
            case (name, statistics) =>
              info(s"  ${name}:")
              info(s"    number of output files: ${statistics.files}")
              info(s"    output file size in bytes: ${statistics.bytes}")
              info(s"    number of output records: ${statistics.records}")
          }
          info(s"  (TOTAL):")
          info(s"    number of output files: ${jobContext.outputStatistics.map(_._2.files).sum}")
          info(s"    output file size in bytes: ${jobContext.outputStatistics.map(_._2.bytes).sum}")
          info(s"    number of output records: ${jobContext.outputStatistics.map(_._2.records).sum}") // scalastyle:ignore
        }

        0
      }
    } catch {
      case NonFatal(t) =>
        executor.stop()
        throw t
    }
  }

  def newJob(jobContext: JobContext): IterativeJob

  def kryoRegistrator: String

  private def newContext(stageInfo: StageInfo)(implicit jobContext: JobContext): RoundContext = {
    val conf = new Configuration(jobContext.sparkContext.hadoopConfiguration)
    conf.set(StageInfo.KEY_NAME, stageInfo.serialize)
    IterativeBatchSparkClient.RoundContext(jobContext.sparkContext.broadcast(conf))
  }
}

object IterativeBatchSparkClient {

  case class JobContext(
    @transient sparkContext: SparkContext)
    extends runtime.JobContext

  case class RoundContext(
    hadoopConf: Broadcasted[Configuration])
    extends runtime.RoundContext {

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

    override def onRoundSubmitted(rc: runtime.RoundContext): Unit = {
      if (Logger.isInfoEnabled) {
        Logger.info(s"Round[${rc}] is submitted.")
      }
    }

    override def onRoundStart(rc: runtime.RoundContext): Unit = {
      if (Logger.isInfoEnabled) {
        Logger.info(s"Round[${rc}] started.")
      }
    }

    override def onRoundCompleted(rc: runtime.RoundContext, result: Try[Unit]): Unit = {
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
