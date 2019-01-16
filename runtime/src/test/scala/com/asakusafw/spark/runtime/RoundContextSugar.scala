/*
 * Copyright 2011-2019 Asakusa Framework Team.
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

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }

import com.asakusafw.bridge.stage.StageInfo

trait RoundContextSugar extends StageInfoSugar {

  def newRoundContext(stageInfo: StageInfo)(implicit jobContext: JobContext): RoundContext = {
    val conf = new Configuration(jobContext.sparkContext.hadoopConfiguration)
    conf.set(StageInfo.KEY_NAME, stageInfo.serialize)

    RoundContextSugar.MockRoundContext(jobContext.sparkContext.broadcast(conf))
  }

  def newRoundContext(
    userName: String = sys.props("user.name"),
    batchId: String = "batchId",
    flowId: String = "flowId",
    stageId: String = null,
    executionId: String = "executionId",
    batchArguments: Map[String, String] = Map.empty)(
      implicit jobContext: JobContext): RoundContext = {

    val stageInfo = newStageInfo(userName, batchId, flowId, stageId, executionId, batchArguments)
    newRoundContext(stageInfo)
  }
}

object RoundContextSugar {

  case class MockRoundContext(hadoopConf: Broadcasted[Configuration]) extends RoundContext {

    private def stageInfo: StageInfo =
      StageInfo.deserialize(hadoopConf.value.get(StageInfo.KEY_NAME))

    override lazy val roundId: Option[String] = {
      Option(stageInfo.getStageId)
    }

    override lazy val toString: String = stageInfo.toString
  }
}
