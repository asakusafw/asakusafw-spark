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
package com.asakusafw.spark.runtime

import java.util.concurrent.{ Executors, ThreadFactory }
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.{ Await, ExecutionContext, ExecutionContextExecutorService }
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }
import org.slf4j.LoggerFactory

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.iterative.launch.IterativeStageInfo
import com.asakusafw.spark.runtime
import com.asakusafw.spark.runtime.graph.Job

trait SparkClient {

  def execute(conf: SparkConf, stageInfo: IterativeStageInfo): Int
}

object SparkClient {

  def ec: ExecutionContextExecutorService = Implicits.ec

  object Implicits {

    implicit lazy val ec: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(
        Executors.newCachedThreadPool({
          val name = "asakusa-executor"
          val group = new ThreadGroup(name)
          val count = new AtomicLong()

          new ThreadFactory() {

            override def newThread(runnable: Runnable): Thread = {
              val thread = new Thread(group, runnable)
              thread.setName(s"${name}-${count.getAndIncrement}")
              thread.setDaemon(true)
              thread
            }
          }
        }))
  }
}

abstract class DefaultClient extends SparkClient {

  val Logger = LoggerFactory.getLogger(getClass)

  override def execute(conf: SparkConf, stageInfo: IterativeStageInfo): Int = {
    require(!stageInfo.isIterative,
      s"This client does not support iterative extension.")

    conf.setHadoopConf(StageInfo.KEY_NAME, stageInfo.getOrigin.serialize)
    execute(conf)
  }

  def execute(conf: SparkConf): Int = {
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", kryoRegistrator)
    conf.set("spark.kryo.referenceTracking", false.toString)

    val sc = SparkContext.getOrCreate(conf)
    try {
      val jobContext = DefaultClient.JobContext(sc)
      val job = newJob(jobContext)
      val hadoopConf = sc.broadcast(sc.hadoopConfiguration)
      val context = DefaultClient.RoundContext(hadoopConf)
      Await.result(job.execute(context)(SparkClient.ec), Duration.Inf)

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
        info(s"    number of output records: ${jobContext.outputStatistics.map(_._2.records).sum}")
      }

      0
    } finally {
      sc.stop()
    }
  }

  def newJob(jobContext: JobContext): Job

  def kryoRegistrator: String
}

object DefaultClient {

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
}
