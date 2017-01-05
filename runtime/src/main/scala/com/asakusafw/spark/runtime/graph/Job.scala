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
package com.asakusafw.spark.runtime
package graph

import scala.concurrent.{ ExecutionContext, Future }

abstract class Job(implicit val jobContext: JobContext) {

  def nodes: Seq[Node]

  def execute(rc: RoundContext)(implicit ec: ExecutionContext): Future[Unit] = {
    Future.sequence(
      nodes.flatMap {
        case source: Source =>
          source.compute(rc).values
        case broadcast: Broadcast[_] =>
          Seq(broadcast.broadcast(rc))
        case action: Action[_] =>
          Seq(action.perform(rc))
        case sink: Sink =>
          Seq(sink.submitJob(rc))
      }).map(_ => ())
  }
}
