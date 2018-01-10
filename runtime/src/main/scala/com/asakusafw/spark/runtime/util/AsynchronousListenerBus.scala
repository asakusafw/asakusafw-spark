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
package com.asakusafw.spark.runtime.util

import scala.util.control.NonFatal

import org.slf4j.LoggerFactory

abstract class AsynchronousListenerBus[L, E](name: String)
  extends ListenerBus[L, E] {

  val Logger = LoggerFactory.getLogger(getClass)

  private val queue = new MessageQueue[E](name, stopOnFail = false) {

    override protected def handleMessage(
      event: E)(
        onSuccess: () => Unit, onFailure: () => Unit): Unit = {
      try {
        postToAll(event)
        onSuccess()
      } catch {
        case NonFatal(t) =>
          if (Logger.isErrorEnabled) {
            Logger.error(t.getMessage, t)
          }
          onFailure()
      }
    }
  }

  def start(): Unit = {
    queue.start()
  }

  def awaitExecution(): Unit = {
    queue.awaitExecution()
  }

  def stop(): Unit = {
    queue.stop(awaitExecution = true, gracefully = true)
  }

  override def post(event: E): Unit = {
    queue.submit(event)
  }
}
