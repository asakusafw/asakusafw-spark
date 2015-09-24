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

import org.apache.spark.broadcast.{ Broadcast => Broadcasted }

trait Broadcast extends Node {

  def broadcast(rc: RoundContext): Future[Broadcasted[_]]

  final def getOrBroadcast(rc: RoundContext): Future[Broadcasted[_]] = {
    Broadcast.getOrBroadcast(this)(rc)
  }
}

object Broadcast {

  private[this] val broadcasted =
    mutable.Map.empty[Broadcast, mutable.Map[RoundContext, Future[Broadcasted[_]]]]

  private def getOrBroadcast(broadcast: Broadcast)(rc: RoundContext): Future[Broadcasted[_]] = {
    synchronized {
      broadcasted
        .getOrElseUpdate(broadcast, mutable.WeakHashMap.empty)
        .getOrElseUpdate(rc, broadcast.broadcast(rc))
    }
  }
}
