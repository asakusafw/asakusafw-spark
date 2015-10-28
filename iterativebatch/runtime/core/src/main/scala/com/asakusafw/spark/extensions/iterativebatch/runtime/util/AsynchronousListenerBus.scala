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
package com.asakusafw.spark.extensions.iterativebatch.runtime.util

import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable

import com.google.common.util.concurrent.ThreadFactoryBuilder

abstract class AsynchronousListenerBus[L, E](name: String)
  extends ListenerBus[L, E] {

  private val executor = new RoundExecutor[E](name) {

    override protected def executeRound(event: E)(onComplete: () => Unit): Unit = {
      postToAll(event)
      onComplete()
    }
  }

  def start(): Unit = {
    executor.start()
  }

  def stop(): Unit = {
    executor.stop(awaitExecution = true, gracefully = true)
  }

  override def post(event: E): Unit = {
    executor.submit(event)
  }
}
