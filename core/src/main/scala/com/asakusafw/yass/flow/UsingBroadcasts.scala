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
package com.asakusafw.yass
package flow

import scala.collection.mutable
import scala.concurrent.Future

import org.apache.spark.Partitioner
import org.apache.spark.broadcast.{ Broadcast => SparkBroadcast }
import org.apache.spark.rdd.RDD

import com.asakusafw.spark.runtime.SparkClient.executionContext
import com.asakusafw.spark.runtime.driver.{ BroadcastId, ShuffleKey }
import com.asakusafw.spark.runtime.rdd._

trait UsingBroadcasts {

  def broadcasts: Map[BroadcastId, Broadcast]

  final def zipBroadcasts(rc: RoundContext): Future[Map[BroadcastId, SparkBroadcast[_]]] = {
    broadcasts.foldLeft(Future.successful(Map.empty[BroadcastId, SparkBroadcast[_]])) {
      case (broadcasts, (broadcastId, broadcast)) =>
        broadcasts.zip(broadcast.getOrBroadcast(rc)).map {
          case (broadcasts, broadcast) =>
            broadcasts + (broadcastId -> broadcast)
        }
    }
  }
}
