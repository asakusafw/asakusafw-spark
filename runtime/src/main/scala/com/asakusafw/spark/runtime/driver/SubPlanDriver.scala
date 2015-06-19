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
package com.asakusafw.spark.runtime.driver

import scala.concurrent.Future

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.rdd.BranchKey

abstract class SubPlanDriver(
    @transient val sc: SparkContext,
    val hadoopConf: Broadcast[Configuration],
    @transient val broadcasts: Map[BroadcastId, Future[Broadcast[_]]]) extends Serializable {

  def label: String

  def execute(): Map[BranchKey, Future[RDD[(ShuffleKey, _)]]]
}
