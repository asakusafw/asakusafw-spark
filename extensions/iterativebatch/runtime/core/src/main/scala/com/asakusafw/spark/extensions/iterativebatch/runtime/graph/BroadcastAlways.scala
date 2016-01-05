/*
 * Copyright 2011-2016 Asakusa Framework Team.
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

import scala.collection.mutable
import scala.concurrent.{ ExecutionContext, Future }

import org.apache.spark.broadcast.{ Broadcast => Broadcasted }

import com.asakusafw.spark.runtime.RoundContext
import com.asakusafw.spark.runtime.graph.Broadcast

trait BroadcastAlways {
  self: Broadcast =>

  @transient
  private val broadcasted: mutable.Map[RoundContext, Future[Broadcasted[_]]] =
    mutable.WeakHashMap.empty

  def getOrBroadcast(rc: RoundContext)(implicit ec: ExecutionContext): Future[Broadcasted[_]] = {
    synchronized(broadcasted.getOrElseUpdate(rc, broadcast(rc)))
  }
}
