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
package com.asakusafw.spark.extensions.iterativebatch.runtime.graph

import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration.Duration

import com.asakusafw.spark.runtime.{ JobContext, RoundContext }
import com.asakusafw.spark.runtime.graph.Job

abstract class IterativeJob(implicit jobContext: JobContext) extends Job {

  def commit(
    origin: RoundContext,
    rcs: Seq[RoundContext])(
      implicit ec: ExecutionContext): Unit = {
    Await.result(doCommit(origin, rcs), Duration.Inf)
  }

  def doCommit(
    origin: RoundContext,
    rcs: Seq[RoundContext])(
      implicit ec: ExecutionContext): Future[Unit]
}
