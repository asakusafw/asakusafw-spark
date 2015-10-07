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
package com.asakusafw.spark.runtime

import org.scalatest.{ BeforeAndAfterEach, Suite }

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast

import com.asakusafw.bridge.stage.StageInfo

trait HadoopConfForEach extends BeforeAndAfterEach {
  self: Suite with SparkForAll =>

  var flowId: String = _
  var hadoopConf: Broadcast[Configuration] = _

  private[this] val nextId = new AtomicInteger(0)

  override def beforeEach(): Unit = {
    try {
      super.beforeEach()
    } finally {
      flowId = s"flowId${nextId.getAndIncrement()}"

      val conf = new Configuration(sc.hadoopConfiguration)
      val stageInfo = new StageInfo(
        sys.props("user.name"), "batchId", flowId, null, "executionId", Map("batcharg" -> "test"))
      conf.set(StageInfo.KEY_NAME, stageInfo.serialize)
      hadoopConf = sc.broadcast(conf)
    }
  }

  override def afterEach(): Unit = {
    try {
      hadoopConf = null
      flowId = null
    } finally {
      super.afterEach()
    }
  }
}
