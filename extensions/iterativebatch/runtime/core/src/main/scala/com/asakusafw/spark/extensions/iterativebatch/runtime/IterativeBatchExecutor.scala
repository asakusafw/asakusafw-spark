/*
 * Copyright 2011-2018 Asakusa Framework Team.
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

import java.util.concurrent.TimeUnit

import scala.collection.mutable.WeakHashMap
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.Duration
import scala.util.{ Failure, Try }
import scala.util.control.NonFatal

import com.asakusafw.spark.runtime.RoundContext
import com.asakusafw.spark.runtime.graph.Job
import com.asakusafw.spark.runtime.util.{
  AsynchronousListenerBus,
  MessageQueue
}

import com.asakusafw.spark.extensions.iterativebatch.runtime.IterativeBatchExecutor._
import com.asakusafw.spark.extensions.iterativebatch.runtime.util.ReadWriteLockedMap

class IterativeBatchExecutor(
  numSlots: Int, stopOnFail: Boolean)(
    job: Job)(implicit ec: ExecutionContext) {

  def this(numSlots: Int)(job: Job)(implicit ec: ExecutionContext) =
    this(numSlots, true)(job)

  def this(stopOnFail: Boolean)(job: Job)(implicit ec: ExecutionContext) =
    this(Int.MaxValue, stopOnFail)(job)

  def this(job: Job)(implicit ec: ExecutionContext) =
    this(Int.MaxValue, true)(job)

  private val results =
    new WeakHashMap[RoundContext, Try[Unit]] with ReadWriteLockedMap[RoundContext, Try[Unit]]
  def result(context: RoundContext): Option[Try[Unit]] = results.get(context)

  private val listenerBus = new ListenerBus("iterativebatch-executor-listenerbus")

  def addListener(listener: Listener): Unit = {
    listenerBus.addListener(listener)
  }

  private val queue = {
    new MessageQueue[RoundContext](
      "iterativebatch-executor", numSlots = numSlots, stopOnFail = stopOnFail) {

      override protected def onStart(): Unit = {
        super.onStart()
        listenerBus.start()
        listenerBus.post(ExecutorStart)
      }

      override protected def onStop(): Unit = {
        listenerBus.post(ExecutorStop)
        listenerBus.stop()
        super.onStop()
      }

      override protected def handleMessage(
        rc: RoundContext)(
          onSuccess: () => Unit, onFailure: () => Unit): Unit = {
        listenerBus.post(RoundStarted(rc))
        try {
          job.execute(rc).onComplete { result =>
            results += rc -> result
            listenerBus.post(RoundCompleted(rc, result))
            if (result.isSuccess) {
              onSuccess()
            } else {
              onFailure()
            }
          }
        } catch {
          case NonFatal(t) =>
            val result = Failure(t)
            results += rc -> result
            listenerBus.post(RoundCompleted(rc, result))
            onFailure()
        }
      }
    }
  }

  def started: Boolean = queue.started
  def running: Boolean = queue.running
  def terminating: Boolean = queue.terminating
  def stopped: Boolean = queue.stopped

  def queueSize: Int = queue.size
  def numRunningBatches: Int = queue.numHandlingMessages

  def start(): Unit = {
    queue.start()
  }

  def stop(awaitExecution: Boolean = false, gracefully: Boolean = false): Seq[RoundContext] = {
    queue.stop(awaitExecution, gracefully)
  }

  def submit(rc: RoundContext): Unit = {
    queue.submit(rc)
    listenerBus.post(RoundSubmitted(rc))
  }
  def submitAll(rcs: Seq[RoundContext]): Unit = {
    queue.submitAll(rcs)
    rcs.foreach(rc => listenerBus.post(RoundSubmitted(rc)))
  }

  def awaitExecution(): Unit =
    queue.awaitExecution()
  def awaitExecution(timeout: Long, unit: TimeUnit): Boolean =
    queue.awaitExecution(timeout, unit)
  def awaitExecution(duration: Duration): Boolean =
    queue.awaitExecution(duration)

  def awaitTermination(): Unit =
    queue.awaitTermination()
  def awaitTermination(timeout: Long, unit: TimeUnit): Boolean =
    queue.awaitTermination(timeout, unit)
  def awaitTermination(duration: Duration): Boolean =
    queue.awaitExecution(duration)
}

object IterativeBatchExecutor {

  sealed trait Event
  case object ExecutorStart extends Event
  case class RoundSubmitted(rc: RoundContext) extends Event
  case class RoundStarted(rc: RoundContext) extends Event
  case class RoundCompleted(rc: RoundContext, result: Try[Unit]) extends Event
  case object ExecutorStop extends Event

  trait Listener {

    def onExecutorStart(): Unit = {}

    def onRoundSubmitted(rc: RoundContext): Unit = {}

    def onRoundStart(rc: RoundContext): Unit = {}

    def onRoundCompleted(rc: RoundContext, result: Try[Unit]): Unit = {}

    def onExecutorStop(): Unit = {}
  }

  class ListenerBus(name: String)
    extends AsynchronousListenerBus[Listener, Event](name) {

    override def postEvent(listener: Listener, event: Event): Unit = {
      event match {
        case ExecutorStart => listener.onExecutorStart()
        case RoundSubmitted(rc) => listener.onRoundSubmitted(rc)
        case RoundStarted(rc) => listener.onRoundStart(rc)
        case RoundCompleted(rc, result) => listener.onRoundCompleted(rc, result)
        case ExecutorStop => listener.onExecutorStop()
      }
    }
  }
}
