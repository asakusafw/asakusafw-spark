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

import scala.collection.mutable.{ SynchronizedMap, WeakHashMap }
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

import com.asakusafw.spark.extensions.iterativebatch.runtime.flow.Sink
import com.asakusafw.spark.extensions.iterativebatch.runtime.util.{
  AsynchronousListenerBus,
  RoundExecutor
}
import com.asakusafw.spark.extensions.iterativebatch.runtime.IterativeBatchExecutor._

abstract class IterativeBatchExecutor(numSlots: Int)(implicit ec: ExecutionContext)
  extends RoundExecutor[RoundContext]("iterativebatch-executor", numSlots = numSlots) {

  def this()(implicit ec: ExecutionContext) = this(Int.MaxValue)

  def sinks: Seq[Sink]

  private val results =
    new WeakHashMap[RoundContext, Try[Unit]] with SynchronizedMap[RoundContext, Try[Unit]]
  def result(context: RoundContext): Try[Unit] = results(context)

  private val listenerBus = new ListenerBus("iterativebatch-executor-listenerbus")

  def addListener(listener: Listener): Unit = {
    listenerBus.addListener(listener)
  }

  override def onStart(): Unit = {
    super.onStart()
    listenerBus.start()
    listenerBus.post(ExecutorStart)
  }

  override def onStop(): Unit = {
    listenerBus.post(ExecutorStop)
    listenerBus.stop()
    super.onStop()
  }

  override protected def executeRound(rc: RoundContext)(onComplete: () => Unit): Unit = {
    listenerBus.post(RoundStarted(rc))
    Future.sequence(sinks.map(_.submitJob(rc))).map(_ => ())
      .onComplete { result =>
        results += rc -> result
        onComplete()
        listenerBus.post(RoundCompleted(rc, result))
      }
  }
}

object IterativeBatchExecutor {

  sealed trait Event
  case object ExecutorStart extends Event
  case class RoundStarted(rc: RoundContext) extends Event
  case class RoundCompleted(rc: RoundContext, result: Try[Unit]) extends Event
  case object ExecutorStop extends Event

  trait Listener {

    def onExecutorStart(): Unit = {}

    def onRoundStart(rc: RoundContext): Unit = {}

    def onRoundCompleted(rc: RoundContext, result: Try[Unit]): Unit = {}

    def onExecutorStop(): Unit = {}
  }

  class ListenerBus(name: String)
    extends AsynchronousListenerBus[Listener, Event](name) {

    override def postEvent(listener: Listener, event: Event): Unit = {
      event match {
        case ExecutorStart => listener.onExecutorStart()
        case RoundStarted(rc) => listener.onRoundStart(rc)
        case RoundCompleted(rc, result) => listener.onRoundCompleted(rc, result)
        case ExecutorStop => listener.onExecutorStop()
      }
    }
  }
}
