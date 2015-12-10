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
package graph

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.{ ExecutionContext, Future }

import org.apache.spark.broadcast.{ Broadcast => Broadcasted }

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.spark.runtime.RoundContext
import com.asakusafw.spark.runtime.graph.Broadcast

trait BroadcastByParameter {
  self: Broadcast =>

  @transient
  private val broadcasted: mutable.Map[Seq[String], Future[Broadcasted[_]]] = mutable.Map.empty

  def getOrBroadcast(rc: RoundContext)(implicit ec: ExecutionContext): Future[Broadcasted[_]] = {
    synchronized {
      val stageInfo = StageInfo.deserialize(rc.hadoopConf.value.get(StageInfo.KEY_NAME))
      val batchArguments = stageInfo.getBatchArguments.toMap
      broadcasted
        .getOrElseUpdate(parameters.toSeq.sorted.map(batchArguments), broadcast(rc))
    }
  }

  def parameters: Set[String]
}
