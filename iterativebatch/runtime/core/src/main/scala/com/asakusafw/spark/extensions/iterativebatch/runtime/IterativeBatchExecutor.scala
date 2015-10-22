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

abstract class IterativeBatchExecutor(slots: Int)(implicit ec: ExecutionContext)
  extends RoundExecutor[RoundContext]("iterativebatch-executor", slots) {

  def this()(implicit ec: ExecutionContext) = this(Int.MaxValue)

  def sinks: Seq[Sink]

  private val results =
    new WeakHashMap[RoundContext, Try[Unit]] with SynchronizedMap[RoundContext, Try[Unit]]
  def result(context: RoundContext): Try[Unit] = results(context)

  protected def executeRound(rc: RoundContext)(onComplete: => Unit): Unit = {
    Future.sequence(sinks.map(_.submitJob(rc))).map(_ => ())
      .onComplete { result =>
        results += rc -> result
        onComplete
      }
  }
}
