/*
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.spark.extensions.iterativebatch.runtime.graph

import scala.concurrent.{ ExecutionContext, Future }

import com.asakusafw.spark.runtime.RoundContext
import com.asakusafw.spark.runtime.graph._

trait IterativeAction[T] extends Node {
  self: CacheStrategy[Seq[RoundContext], Future[T]] =>

  protected def doPerform(
    origin: RoundContext, rcs: Seq[RoundContext])(implicit ec: ExecutionContext): Future[T]

  final def perform(
    origin: RoundContext, rcs: Seq[RoundContext])(implicit ec: ExecutionContext): Future[T] = {
    getOrCache(rcs, doPerform(origin, rcs))
  }
}
