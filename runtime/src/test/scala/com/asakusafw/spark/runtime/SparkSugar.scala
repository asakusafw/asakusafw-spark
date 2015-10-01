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

import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach }
import org.scalatest.Suite

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import com.asakusafw.bridge.stage.StageInfo

trait SparkSugar extends BeforeAndAfterAll with BeforeAndAfterEach { self: Suite =>

  // for all
  var sc: SparkContext = _

  // for each
  var flowId: String = _
  var hadoopConf: Broadcast[Configuration] = _

  override def beforeAll(): Unit = {
    try {
      super.beforeAll()
    } finally {
      val conf = new SparkConf
      conf.setMaster("local[8]")
      conf.setAppName(getClass.getName)
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.set("spark.kryo.registrator", kryoRegistrator)

      conf.set(Props.Parallelism, 8.toString)

      sc = new SparkContext(conf)
    }
  }

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

  override def afterAll(): Unit = {
    try {
      sc.stop
      sc = null
    } finally {
      super.afterAll()
    }
  }

  def kryoRegistrator: String = "com.asakusafw.spark.runtime.serializer.KryoRegistrator"
}
