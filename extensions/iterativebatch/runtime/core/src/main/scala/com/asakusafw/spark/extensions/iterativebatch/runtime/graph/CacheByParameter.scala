/*
 * Copyright 2011-2021 Asakusa Framework Team.
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

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.spark.runtime.RoundContext
import com.asakusafw.spark.runtime.graph.CacheStrategy

trait CacheByParameter[T] extends CacheStrategy[RoundContext, T] {

  @transient
  private val values: mutable.Map[Seq[String], T] = mutable.Map.empty

  override def getOrCache(rc: RoundContext, v: => T): T = synchronized {
    val stageInfo = StageInfo.deserialize(rc.hadoopConf.value.get(StageInfo.KEY_NAME))
    val batchArguments = stageInfo.getBatchArguments.toMap
    values.getOrElseUpdate(parameters.toSeq.sorted.map(batchArguments), v)
  }

  def parameters: Set[String]
}
