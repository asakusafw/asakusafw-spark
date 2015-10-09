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
package com.asakusafw.yass.runtime

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.spark.runtime

trait RoundContextSugar {

  def newRoundContext(stageInfo: StageInfo)(implicit sc: SparkContext): RoundContext = {
    val conf = new Configuration(sc.hadoopConfiguration)
    conf.set(StageInfo.KEY_NAME, stageInfo.serialize)

    RoundContextSugar.MockRoundContext(sc.broadcast(conf))
  }

  def newRoundContext(
    userName: String = sys.props("user.name"),
    batchId: String = "batchId",
    flowId: String = "flowId",
    executionId: String = "executionId",
    batchArguments: Map[String, String] = Map("batcharg" -> "test"))(
      implicit sc: SparkContext): RoundContext = {

    val stageInfo = new StageInfo(userName, batchId, flowId, null, executionId, batchArguments)
    newRoundContext(stageInfo)
  }
}

object RoundContextSugar {

  case class MockRoundContext(hadoopConf: Broadcasted[Configuration]) extends RoundContext
}
