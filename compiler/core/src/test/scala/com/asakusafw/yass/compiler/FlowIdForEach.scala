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
package com.asakusafw.yass.compiler

import org.scalatest.{ BeforeAndAfterEach, Suite }

import java.util.concurrent.atomic.AtomicInteger

trait FlowIdForEach extends BeforeAndAfterEach {
  self: Suite =>

  var flowId: String = _

  private[this] val nextId = new AtomicInteger(0)

  override def beforeEach(): Unit = {
    try {
      super.beforeEach()
    } finally {
      flowId = s"flowId${nextId.getAndIncrement()}"
    }
  }

  override def afterEach(): Unit = {
    try {
      flowId = null
    } finally {
      super.afterEach()
    }
  }
}
