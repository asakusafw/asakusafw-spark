/*
 * Copyright 2011-2017 Asakusa Framework Team.
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

import java.util.concurrent.CopyOnWriteArrayList

trait ListenerBus[L, E] {

  private val listeners = new CopyOnWriteArrayList[L]()

  final def addListener(listener: L): Unit = {
    listeners.add(listener)
  }

  def post(event: E): Unit

  protected final def postToAll(event: E): Unit = {
    val iter = listeners.iterator
    while (iter.hasNext) {
      postEvent(iter.next(), event)
    }
  }

  protected def postEvent(listener: L, event: E): Unit
}
