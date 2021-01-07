/*
 * Copyright 2011-2021 Asakusa Framework Team.
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

import java.util.ServiceLoader
import java.util.concurrent.{ Executors, ThreadFactory }
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConversions._
import scala.concurrent.{ Await, ExecutionContext, ExecutionContextExecutorService }
import scala.concurrent.duration.Duration
import scala.reflect.{ classTag, ClassTag }
import scala.util.{ Failure, Success, Try }

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.iterative.launch.IterativeStageInfo
import com.asakusafw.spark.runtime
import com.asakusafw.spark.runtime.SparkClient._
import com.asakusafw.spark.runtime.graph.Job
import com.asakusafw.spark.runtime.util.{ ListenerBus => _, _ }

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

  sealed trait Event
  case class Configure(conf: SparkConf) extends Event
  case class JobStart(jobContext: JobContext) extends Event
  case class JobCompleted(jobContext: JobContext, result: Try[Int]) extends Event

  trait Listener {

    def onConfigure(conf: SparkConf): Unit = {}

    def onJobStart(jobContext: JobContext): Unit = {}

    def onJobCompleted(jobContext: JobContext, result: Try[Int]): Unit = {}
  }

  class ListenerBus(name: String)
    extends AsynchronousListenerBus[Listener, Event](name) {

    override def postEvent(listener: Listener, event: Event): Unit = {
      event match {
        case Configure(conf) => listener.onConfigure(conf)
        case JobStart(jobContext) => listener.onJobStart(jobContext)
        case JobCompleted(jobContext, result) => listener.onJobCompleted(jobContext, result)
      }
    }
  }

  def loadListeners[L: ClassTag](): Seq[L] = {
    loadListeners[L](Thread.currentThread.getContextClassLoader)
  }

  def loadListeners[L: ClassTag](cl: ClassLoader): Seq[L] = {
    ServiceLoader.load(classTag[L].runtimeClass.asInstanceOf[Class[L]], cl).toSeq
  }
}

abstract class DefaultClient extends SparkClient {

  override def execute(conf: SparkConf, stageInfo: IterativeStageInfo): Int = {
    require(!stageInfo.isIterative,
      s"This client does not support iterative extension.")

    conf.setHadoopConf(StageInfo.KEY_NAME, stageInfo.getOrigin.serialize)
    execute(conf)
  }

  def execute(conf: SparkConf): Int = {
    val listenerBus = new ListenerBus(getClass.getName)
    loadListeners[Listener]().foreach(listenerBus.addListener)
    listenerBus.start()

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", kryoRegistrator)
    conf.set("spark.kryo.referenceTracking", false.toString)

    listenerBus.post(Configure(conf))

    val sc = SparkContext.getOrCreate(conf)
    try {
      val jobContext = DefaultClient.JobContext(sc)

      listenerBus.post(JobStart(jobContext))

      val result = Try {
        val job = newJob(jobContext)
        val hadoopConf = sc.broadcast(sc.hadoopConfiguration)
        val context = DefaultClient.RoundContext(hadoopConf)
        Await.result(job.execute(context)(SparkClient.ec), Duration.Inf)
        0
      }

      listenerBus.post(JobCompleted(jobContext, result))

      result match {
        case Success(code) => code
        case Failure(t) => throw t
      }
    } finally {
      listenerBus.awaitExecution()
      try {
        sc.stop()
      } finally {
        listenerBus.stop()
      }
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
