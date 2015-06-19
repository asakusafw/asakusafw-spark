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

import org.scalatest.BeforeAndAfterEach
import org.scalatest.Suite

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import com.asakusafw.bridge.stage.StageInfo

trait SparkSugar extends BeforeAndAfterEach { self: Suite =>

  var sc: SparkContext = _
  var hadoopConf: Broadcast[Configuration] = _

  override def beforeEach() {
    try {
      val conf = new SparkConf
      conf.setMaster("local[*]")
      conf.setAppName(getClass.getName)
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.set("spark.kryo.registrator", kryoRegistrator)

      val stageInfo = new StageInfo(
        sys.props("user.name"), "batchId", "flowId", null, "executionId", Map("batcharg" -> "test"))
      conf.setHadoopConf(StageInfo.KEY_NAME, stageInfo.serialize)

      sc = new SparkContext(conf)
      hadoopConf = sc.broadcast(sc.hadoopConfiguration)
    } finally {
      super.beforeEach()
    }
  }

  def kryoRegistrator: String = "com.asakusafw.spark.runtime.serializer.KryoRegistrator"

  override def afterEach() {
    try {
      super.afterEach()
    } finally {
      hadoopConf = null
      sc.stop
      sc = null
    }
  }
}
